async      = require 'async'
mongo      = require 'mongoskin'
toObjectId = mongo.helper.toObjectID
# db         = mongo.db "mongodb://localhost/app_mae", native_parser: true
db2        = mongo.db "mongodb://localhost/mae_analysis", native_parser: true

Server         = mongo.Server;
ReplSetServers = mongo.ReplSetServers
Db             = mongo.Db;
replSet        = new ReplSetServers([
  new Server('10.232.50.196', 27017)
  new Server('10.221.144.69', 27017)
  new Server('10.221.144.159', 27017) #primary
], {read_secondary:true})
db = new Db('app_mae', replSet, {w:0, native_parser: true})

db.bind 'tb_module_scrm_member'
db.bind 'tb_module_scrm_logs'

db2.bind 'tb_module_scrm_member'
db2.bind 'tb_module_scrm_logs'

Array::distinct = ->
  u = {}
  a = []

  for i in [0..@length-1]
    if u.hasOwnProperty @[i]
      continue
    a.push @[i]
    u[@[i]] = 1

  return a


dbq = async.queue (task, callback) ->
  task.run callback
, 10

db.authenticate 'appmae', 'Aim123789', ->
  require('./memberCard')(db, db2, toObjectId, dbq).run 0, 1407859254, (err) ->
    console.log err
  # require('./bookcar')(db, db2, toObjectId, dbq).run 0, 1407859254, (err) ->
  #   console.log err
  # require('./crewbattle')(db, db2, toObjectId, dbq).run 0, 1407859254, (err) ->
  #   console.log err