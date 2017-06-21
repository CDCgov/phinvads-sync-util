require "elasticsearch"
require 'uri'
require 'json'
require 'logger'
require 'csv'

class VadsSync

  # Create a new sync utility
  # @param: es_uri  url to the elasticsearch server to update
  # @param: vads_uri url to the phinvads hessian api endpoint
  # @param: force_reload if an item is already found in elasticsearch this controls whether or
  #         not to force a reload item (defaults to false)
  # @param: use_latest when syncing valuesets this controls whether or not to sync just the latest version
  #                    or whether ot sync all versions (defaults to true)
  def initialize(es_uri, vads_uri, force_reload=false, use_latest=true)
    @es_client = create_es_client(es_uri)
    @vads_client = create_vads_client(vads_uri)
    @force=force_reload
    @use_latest = use_latest
    @max_vs_concept_length=100000
    ensure_indexes
  end

  # sync all of the code systems in PHIN VADS to elastic search
  def sync_code_systems()
    logger.debug "Sync code systems"
    @code_systems = @vads_client.getAllCodeSystems.getCodeSystems
    @code_systems.each do |cs|
      logger.debug "working code system #{cs.name}"
      json = code_system_to_json(cs)
      es_cs = get_code_system_from_es(cs.oid)
      if !es_cs || @force
        logger.debug "calling syncing codes for #{cs.name}"
        sync_code_system_codes(cs.oid)
        @es_client.update index: 'code_systems',  type: "code_system",  id: cs.oid,  body: { doc: json, doc_as_upsert: true }
      end

    end
  end

  # Sync an individual code system into elastic search
  def sync_code_system(oid)
    logger.debug "Sync code systems"
    cs = @vads_client.getCodeSystemByOid(oid).getCodeSystem
    logger.debug "working code system #{cs.name}"
    json = code_system_to_json(cs)
    es_cs = get_code_system_from_es(cs.oid)
    if !es_cs || @force
      logger.debug "calling syncing codes for #{cs.name}"
      sync_code_system_codes(cs.oid)
      logger.debug "adding code_system to index"
      @es_client.update index: 'code_systems',  type: "code_system",  id: cs.oid,  body: { doc: json, doc_as_upsert: true }
    end

  end

  # Sync all of the valuesets and all of the versions into elastic search
  def sync_value_sets()
    logger.debug "Sync valuesets "
    #get all of the versions and valuesets and map them together
    #this is cleaner and easier than getting the vs and the getting all of it's versions
    # separately
    valuesets = @vads_client.getAllValueSets.getValueSets
    versions = @vads_client.getAllValueSetVersions.getValueSetVersions
    temp = {}
    valuesets.each do |vs|
      temp[vs.oid] = {valueset: vs, versions:[]}
    end
    versions.each do |ver|
      temp[ver.valueSetOid][:versions] << ver
    end
    @valuesets = temp.values

    @valuesets.each do |vs|
      logger.debug "working valueset #{vs[:valueset].name}"
      json = value_set_to_json(vs)
      es_vs = es_get('valuesets', 'valueset', vs[:valueset].oid)
      if !es_vs || @force
        sync_valueset_versions(vs)
        @es_client.update index: 'valuesets',  type: "valueset",  id: vs[:valueset].oid,  body: { doc: json, doc_as_upsert: true }
      end
    end

  end

  # Sync the codes for a valueset version into elastic search
  def sync_valueset_versions(vs)
    logger.debug "updating valueset versions "
    vset = vs[:valueset]
    versions = vs[:versions] || []
    versions.sort!{|a,b| b.versionNumber <=> a.versionNumber}
    versions = [versions[0]] if @use_latest
    versions.each do |ver|
      es_vs = es_get('valueset_versions', vset.oid, ver.versionNumber)
      if !es_vs || @force
        logger.debug "getting codes for version #{ver.versionNumber}"

        json = valueset_to_fhir(vset, ver.versionNumber, [])
        @es_client.update index: 'valueset_versions',  type: vset.oid,  id: ver.versionNumber,  body: { doc: json, doc_as_upsert: true }
        sync_valueset_codes(vset.oid, ver.versionNumber, ver.id)
      end
    end
  end

  #sync a valueset into elastic search, if a version is not supplied the latest version will be used
  def sync_valueset(oid,version=nil)
    vset = @vads_client.getValueSetByOid(oid).getValueSet
    versions = @vads_client.getValueSetVersionsByValueSetOid(oid).getValueSetVersions
    if version == "latest"
      versions = [versions[0]]
    elsif version
      ver = versions.find{|v| v.versionNumber == version}
      versions = [ver].compact
    end
    versions.each do |ver|
      es_vs = es_get('valueset_versions', vset.oid, ver.versionNumber)
      if !es_vs || @force
        logger.debug "getting codes for version #{ver.versionNumber}"

        json = valueset_to_fhir(vset, ver.versionNumber, [])
        @es_client.update index: 'valueset_versions',  type: vset.oid,  id: ver.versionNumber,  body: { doc: json, doc_as_upsert: true }
        sync_valueset_codes(vset.oid, ver.versionNumber, ver.id)
      end
    end
  end

  # Sync all of the codes for a code system into elastic search
  def sync_code_system_codes(oid)
    start = Time.now
    page = 1
    count = 0
    limit = 1000
    loop do
      logger.debug "calling get concepts"
      dto = @vads_client.getCodeSystemConceptsByCodeSystemOid(oid, page, limit)
      length = dto.getCodeSystemConcepts.length
      logger.debug "syncing concepts #{count} to #{count + length} of #{dto.getTotalResults()} "
      count = count + length
      page = page + 1
      bulks = []
      dto.getCodeSystemConcepts.each do |con|
        json = code_system_code_to_json(con)
        bulks << {update: {_index: 'codes',   _type: oid, _id: con.id,  data: { doc: json, doc_as_upsert: true }}}
      end
      logger.debug "bulk adding codes to ES"
      @es_client.bulk body: bulks
      break if count >= dto.getTotalResults()
    end
    logger.debug "Took #{Time.now - start}"
  end

  #sync all of the codes for a valueset version into elastic search
  def sync_valueset_codes(oid, version_number, version_id)
    start = Time.now
    count = 0
    page = 1
    limit = 1000
    loop do
      dto = @vads_client.getValueSetConceptsByValueSetVersionId(version_id, page, limit)
      break if dto.getTotalResults() > @max_vs_concept_length
      length = dto.getValueSetConcepts ? dto.getValueSetConcepts.length : 0
      logger.debug "getting vs codes #{count} to #{count + length} of #{dto.getTotalResults()} "
      count = count +  length
      page = page + 1
      codes = []
      dto.getValueSetConcepts.each do |code|
        codes << valueset_code_to_json(code)
      end

      @es_client.update index: 'valueset_versions',
      type: oid,
      id: version_number,
      body: { script: {inline: "ctx._source.expansion.contains.add( params.contains)",
        params: {
        contains: codes  }
        }
      }
      break if count >= dto.getTotalResults()
    end
    logger.debug "Took #{Time.now - start}"

  end

  # try and get a code system from elastic search
  def get_code_system_from_es(oid)
    es_get('code_systems','code_system',  oid)
  end
  # try and get a valueset from elastic search
  def get_vs_from_es(oid)
    es_get('value_sets', 'value_set', oid)
  end

  # try and get an object from elasticsearch
  def es_get(index, type, id)
    begin
      return @es_client.get index: index, type: type , id: id
    rescue
    end
  end

  # map a phinvads code system object into a json doucument to add to elasticsearch
  def code_system_to_json(code_system)
    keys = [:oid,:id,:name,:definitionText,:status,:statusDate,:version,
      :versionDescription,:acquiredDate,:effectiveDate,:expiryDate,
      :assigningAuthorityVersionName,:assigningAuthorityReleaseDate,
      :distributionSourceVersionName,:distributionSourceReleaseDate,
      :distributionSourceId,:sdoCreateDate,:lastRevisionDate,:sdoReleaseDate]

    hash = {}
    keys.each do |k|
      hash[k] = code_system.send k
    end
    hash
  end

  # Map a code system code to a json object for insertion into elastic search
  def code_system_code_to_json(concept)
    keys = [:id,:name,:codeSystemOid,:conceptCode,:sdoPreferredDesignation,
      :definitionText,:preCoordinatedFlag,:preCoordinatedConceptNote,
      :sdoConceptCreatedDate,:sdoConceptRevisionDate,:status,:statusDate,
      :sdoConceptStatus,:sdoConceptStatusDate,:supersededByCodeSystemConceptId,
      :umlsCui,:umlsAui]
    hash = {}
    keys.each do |k|
      hash[k] = concept.send k
    end
    hash
  end

  # map a phinvads valueset into a josn document for insertion in eastic search
  def value_set_to_json(vs)
    keys = [:id,:oid,:name,:code,:status,
      :statusDate,:definitionText,:scopeNoteText,
      :assigningAuthorityId,:valueSetCreatedDate,
      :valueSetLastRevisionDate]
    hash = {}
    keys.each do |k|
      hash[k] = vs[:valueset].send k
    end
    hash[:versions] = []
    version_keys = [:id,:valueSetOid,:versionNumber,:description,
      :status,:statusDate,:assigningAuthorityText,:assigningAuthorityReleaseDate,:noteText,
      :effectiveDate,:expiryDate]

    (vs[:versions] || []).each do |ver|
      h = {}
      version_keys.each do |k|
        h[k] = ver.send k
      end
      hash[:versions] << h
    end
    hash
  end

  #map a valueset code into a json representation for insertion in elastic search
  def valueset_code_to_json(code)

    json = {}
    json[:system] =  code.codeSystemOid
    json[:code] =   code.conceptCode
    json[:display] =   code.codeSystemConceptName
    json[:description] =   code.definitionText
    json[:valueSetVersionId] =   code.valueSetVersionId
    json
  end

   # map a valueset version and it's codes into a json document for insertion in elastic search
  def valueset_to_fhir(vs, version, codes = [])
    json = {version: version,
      name: vs.name,
      status: vs.status,
      description: vs.definitionText,
      publisher: "PHIN-VADS" ,
      identifier:  [{system: 'urn:ietf:rfc:3986', value: 'urn:oid:' + vs.oid}],
      expansion: {identifier: nil, timestamp: nil, contains: codes }
    }
    json
  end



  # create a new Elasticsearch client
  def create_es_client(uri)
    Elasticsearch::Client.new(host: uri, adapter: :net_http)
  end


  # create a new phinvads api client, this is a java object pulled in from the
  #phinvads api jar file
  def create_vads_client(uri)
    factory = com.caucho.hessian.client.HessianProxyFactory.new
    factory.create(Java::GovCdcVocabService::VocabService.java_class,uri)
  end

  # make sure all of the indexes that are required are in elasticsearch
  # trying to put documents in es without the index being there throws an
  # exception
  def ensure_indexes()
    [:codes,:code_systems,:valuesets,:valueset_versions].each do |index|
      unless @es_client.indices.exists? index: index
        @es_client.indices.create index: index
      end
    end
  end

  # create a logger for printing messages to stdout, slightly better than puts
  def logger
    @@logger ||= Logger.new(STDOUT)
  end
end
