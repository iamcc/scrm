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


dbq = async.queue (task, callback) ->
  task.run callback
, 10

db.authenticate 'appmae', 'Aim123789', ->
  require('./openId2')(db, db2, toObjectId, dbq).run 0, +new Date/1000, (err) ->
    console.log err
#   require('./tap')(db, db2, toObjectId, dbq).run 0, +new Date/1000, (err) ->
#     console.log err


# require('./piwik')(db, db2, toObjectId, dbq).run 0, +new Date/1000, (err) ->
#   console.log err

# 更新 Sex
# db2.tb_module_scrm_member.find({}).toArray (err, members) ->
#   oids = members.map (member) -> member.OpenId

#   fnAuth = (cb) ->
#     db.authenticate 'appmae', 'Aim123789', cb
#   fnModuleMember = (cb) ->
#     db.tb_module_member.find({'OAuth.OpenID': $in: oids}).toArray (err, docs) ->
#       console.log err if err
#       async.each docs, (doc, cbEach) ->
#         db2.tb_module_scrm_member.update {OpenId: doc.OAuth.OpenID}, {$set: Sex: doc.Sex}, (err) ->
#           console.error err if err
#           cbEach()
#       , cb
#   fnOAuthMember = (cb) ->
#     db.tb_module_oauth_member.find({'WxOpenID': $in: oids}).toArray (err, docs) ->
#       console.log err if err
#       async.each docs, (doc, cbEach) ->
#         sex = doc.RawData and doc.RawData.sex or ''

#         db2.tb_module_scrm_member.update {OpenId: doc.WxOpenID}, {$set: Sex: sex}, (err) ->
#           console.error err if err
#           cbEach()
#       , cb
#   fnOAuthGuest = (cb) ->
#     oidArr = []
#     i = 0

#     while (tmp = oids.slice(i, 1000)).length
#       oidArr.push tmp
#       i += tmp.length

#     async.each oidArr, (_oids, cbOids) ->
#       db.tb_module_oauth_guest.find({'WxOpenID': $in: _oids}).toArray (err, docs) ->
#         console.log err if err
#         async.each docs, (doc, cbEach) ->
#           sex = doc.RawData and doc.RawData.sex or ''

#           db2.tb_module_scrm_member.update {OpenId: doc.WxOpenID}, {$set: Sex: sex}, (err) ->
#             console.error err if err
#             cbEach()
#         , cbOids
#     , cb

#   async.series [fnAuth, fnModuleMember, fnOAuthGuest, fnOAuthMember], ->
#     console.log 'done'

# 更新Pages
# fnTmpPages = (cb) ->
#   db2.tb_module_scrm_logs.aggregate [
#     { 
#       $match: 
#         $and : [
#           {PiwikData : $exists: true} 
#           {AddTime   : $gt: 0}
#           {AddTime   : $lt: +new Date/1000}
#         ]
#     }
#     { $unwind: '$PiwikData.Details' }
#     {
#       $group:
#         _id:
#           OpenId: '$OpenId'
#           TID: '$TID'
#         Pages: $addToSet: '$PiwikData.Details.Title'
#     }
#     {$out: 'tmp_pages'}
#   ], (err) ->
#     db2.bind 'tmp_pages'
#     db2.tmp_pages.find({}).toArray (err, docs) ->
#       async.eachLimit docs, 10, (doc, cbEach) ->
#         db2.tb_module_scrm_member.update {TID: doc._id.TID, OpenId: doc._id.OpenId}
#         , {$addToSet: Pages: $each: doc.Pages}
#         , cbEach
#       , cb

# async.series [fnTmpPages], (err) ->
#   console.log 'done', err
#   process.exit()