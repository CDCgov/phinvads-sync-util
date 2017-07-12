require 'slop'
require 'vads_sync'

opts = Slop.parse do |o|
  o.string '-e', '--elasticsearch', 'elastic search host', default: 'http://localhost:9200'
  o.string '-v' , '--vads', 'PHINVADS api url', default: "https://phinvads.cdc.gov/vocabService/v2"
  o.string '-o', '--operation', 'operation to run (sync_all, sync_vs[:oid:version], sync_cs[:oid], csv_cs[:oid], csv_vs[:oid][:version]) ', default: 'sync_all'
  o.bool '-f', '--force', "force reindex", default: false
  o.string '-d', '--dir', "csv output directory", default: "./csv"
end

sync = VadsSync.new(opts[:elasticsearch], opts[:vads], opts[:force])
operation, oid, version, file = opts[:operation].split(":")
if operation == "sync_all"
  sync.sync_code_systems
  sync.sync_value_sets
elsif operation == "sync_vs"
  oid ? sync.sync_valueset(oid,version) : sync.sync_value_sets
elsif operation == "sync_cs"
  oid ? sync.sync_code_system(oid) : sync.sync_code_systems
elsif operation == "csv_cs"
  oid ? sync.sync_code_system_to_csv_by_oid(oid, opts[:dir]) : sync.sync_code_systems_to_csv(opts[:dir])
elsif operation == "csv_vs"
  oid ? sync.sync_value_set_to_csv(oid,version, opts[:dir]) : sync.sync_value_sets_to_csv(opts[:dir])
elsif operation == "csv_vs_meta"
   sync.sync_value_set_metatdata_to_csv(opts[:dir])
 elsif operation == "csv_cs_meta"
    sync.sync_code_system_metadata_to_csv(opts[:dir])
else
  puts "Unknown operation #{operation}"
  puts opts
end
