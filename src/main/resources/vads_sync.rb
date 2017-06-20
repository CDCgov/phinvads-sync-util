require "elasticsearch"
require 'uri'
require 'json'
require 'logger'

class VadsSync
  def initialize(es_uri, vads_uri, force_reload=false, use_latest=true)
    @es_client = create_es_client(es_uri)
    @vads_client = create_vads_client(vads_uri)
    @force=force_reload
    @use_latest = use_latest
    @max_vs_concept_length=100000
    ensure_indexes
  end

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

  def sync_code_system(oid)
    logger.debug "Sync code systems"
    cs = @vads_client.getCodeSystemByOid(oid).getCodeSystem
    logger.debug "working code system #{cs.name}"
    json = code_system_to_json(cs)
    es_cs = get_code_system_from_es(cs.oid)
    if !es_cs || @force
      logger.debug "calling syncing codes for #{cs.name}"
      sync_code_system_codes(cs.oid)
      @es_client.update index: 'code_systems',  type: "code_system",  id: cs.oid,  body: { doc: json, doc_as_upsert: true }
    end

  end

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

  def sync_code_system_codes(oid)
    start = Time.now
    page = 1
    count = 0
    limit = 10000
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
      @es_client.bulk body: bulks
      break if count >= dto.getTotalResults()
    end
    logger.debug "Took #{Time.now - start}"
  end

  def sync_valueset_codes(oid, version_number, version_id)
    start = Time.now
    count = 0
    page = 1
    limit = 10000
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

  def get_code_system_from_es(oid)
    es_get('code_systems','code_system',  oid)
  end

  def get_vs_from_es(oid)
    es_get('value_sets', 'value_set', oid)
  end

  def es_get(index, type, id)
    begin
      return @es_client.get index: index, type: type , id: id
    rescue
    end
  end

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

  def valueset_code_to_json(code)

    json = {}
    json[:system] =  code.codeSystemOid
    json[:code] =   code.conceptCode
    json[:display] =   code.codeSystemConceptName
    json[:description] =   code.definitionText
    json[:valueSetVersionId] =   code.valueSetVersionId
    json
  end

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

  def create_es_client(uri)
    Elasticsearch::Client.new(host: uri)
  end

  def create_vads_client(uri)
    factory = com.caucho.hessian.client.HessianProxyFactory.new
    factory.create(Java::GovCdcVocabService::VocabService.java_class,uri)
  end

  def ensure_indexes()
    [:codes,:code_systems,:valuesets,:valueset_versions].each do |index|
      unless @es_client.indices.exists? index: index
        @es_client.indices.create index: index
      end
    end
  end

  def logger
    @@logger ||= Logger.new(STDOUT)
  end
end