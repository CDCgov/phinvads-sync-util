require 'rubygems'
require_relative "repo/hessian/hessian/3.1.3/hessian-3.1.3.jar"
require_relative "repo/phin_vads/vocabService/0.0.0/vocabService-0.0.0.jar"
require_relative 'lib/vads_sync'

JAVA_SYSTEM = Java::JavaLang::System
PROXY = ENV["http_proxy"] || ENV["HTTP_PROXY"]
if PROXY
  uri = URI(PROXY)
  PROXY_HOST = uri.host
  PROXY_PORT = uri.port || 80
  JAVA_SYSTEM.setProperty("http.proxyHost",PROXY_HOST)
  JAVA_SYSTEM.setProperty("http.proxyPort",PROXY_PORT.to_s)
  JAVA_SYSTEM.setProperty("https.proxyHost",PROXY_HOST)
  JAVA_SYSTEM.setProperty("https.proxyPort",PROXY_PORT.to_s)
  JAVA_SYSTEM.setProperty("http.nonProxyHosts","localhost|127.0.0.1")
  JAVA_SYSTEM.setProperty("https.nonProxyHosts","localhost|127.0.0.1")
end


namespace :sync do
DEFAULTS = {"es" => "localhost:9200",
            "vads" =>  "https://phinvads.cdc.gov/vocabService/v2",
            "dir" => "./csv",
            "force" => false}

task :vs_metadata, [:dir,:vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  sync = VadsSync.new(nil, vads)
  sync.sync_value_set_metatdata_to_csv(args.dir || DEFAULTS["dir"])
end

task :cs_metadata, [:dir,:vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  sync = VadsSync.new(nil, vads)
  sync.sync_code_system_metatdata_to_csv(args.dir || DEFAULTS["dir"])
end

task :vs_to_csv, [:oid, :version, :dir, :vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  force = args.force == "true"
  dir = args.dir || DEFAULTS["dir"]
  sync = VadsSync.new(nil, vads)
  if args.oid
    sync.sync_value_set_to_csv(args.oid, args.version || "latest",dir)
  else
    sync.sync_value_sets_to_csv(dir)
  end
end

task :cs_to_csv, [:oid, :dir, :vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  dir = args.dir || DEFAULTS["dir"]
  sync = VadsSync.new(nil, vads)
  if args.oid
    sync.sync_code_system_to_csv_by_oid(args.oid, dir)
  else
    sync.sync_code_systems_to_csv(dir)
  end
end


task :vs_to_es, [:oid, :version, :force, :es, :vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  es = args.es || DEFAULTS["es"]
  dir = args.dir || DEFAULTS["dir"]
  force = (args.force == 'true' || args.force == true)
  puts args.force == "true"
  sync = VadsSync.new(es, vads, force )
  if args.oid
    puts " hey"
    sync.sync_valueset(args.oid, args.version || "latest")
    puts "there"
  else
    sync.sync_value_sets
  end
end

task :cs_to_es, [:oid, :force, :es, :vads] do |_t, args|
  vads = args.vads || DEFAULTS["vads"]
  es = args.es || DEFAULTS["es"]
  dir = args.dir || DEFAULTS["dir"]
  force = args.force == "true"
  sync = VadsSync.new(es, vads, force )
  if args.oid
    sync.sync_code_system(args.oid)
  else
    sync.sync_code_systems
  end
end

end
