require 'slop'
require 'vads_sync'

opts = Slop.parse do |o|
  o.string '-e', '--elasticsearch', 'elastic search host', default: 'http://localhost:9200'
  o.string '-v' , '--vads', 'PHINVADS api url', default: "https://phinvads.cdc.gov/vocabService/v2"
  o.string '-o', '--operation', 'operation to run (sync_all, sync_vs[:oid:version], sync_cs[:oid]) ', default: 'sync_all'
  o.bool '-f', '--force', "force reindex", default: false

end

sync = VadsSync.new(opts[:elasticsearch], opts[:vads], opts[:force])
operation, oid, version = opts[:operation].split(":")
if operation == "sync_all"
  sync.sync_code_systems 
  sync.sync_value_sets 
elsif operation == "sync_vs"
  oid ? sync.sync_valueset(oid,version) : sync.sync_value_sets
elsif operation == "sync_cs"
  oid ? sync.sync_code_system(oid) : sync.sync_code_systems
else
  puts "Unknown operation #{operation}"
  puts opts
end
