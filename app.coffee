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

db.bind 'tb_module_member'
db.bind 'tb_module_oauth_guest'

db2.bind 'tb_module_scrm_member'
db2.bind 'tb_module_scrm_logs'
db2.bind 'tmp_analysis_logs'

dbq = async.queue (task, callback) ->
  task.run callback
, 10

dbq.___count = 0

Array::distinct = ->
  u = {}
  a = []

  for i in [0..@length-1]
    if u.hasOwnProperty @[i]
      continue
    a.push @[i]
    u[@[i]] = 1

  return a

# 营销活动
activity = require('./activity') db, db2, toObjectId, dbq

# 爬龙舟
boat = require('./boat') db, db2, toObjectId, dbq

# 垂钓网
fishing = require('./fishing') db, db2, toObjectId, dbq

# 邀请函
invitative = require('./invitative') db, db2, toObjectId, dbq

# 公益树
plant = require('./plant') db, db2, toObjectId, dbq

# 摇一摇
shake = require('./shake') db, db2, toObjectId, dbq

# 星光大道
signin = require('./signin') db, db2, toObjectId, dbq

# 有奖调查
survey = require('./survey') db, db2, toObjectId, dbq

# 社会化拼团
ugcgroup = require('./ugcgroup') db, db2, toObjectId, dbq

# 微信机签到
tvCheckIn = require('./tvCheckIn') db, db2, toObjectId, dbq

# 颠球大赛
tap = require('./tap') db, db2, toObjectId, dbq

updateMembersNameMobile = ->
  console.log 'begin update members name and mobile'
  dbq.___count++

  db2.tb_module_scrm_member.find { Name: $exists: false }
  .toArray (err, members) ->
    members.forEach (member) ->
      if member.Contacts.length
        contact = member.Contacts.slice(-1)[0]

        dbq.push {
          run: (cbQueue) ->
            db2.tb_module_scrm_member.update { _id: member._id }
            , {
              $set:
                Name   : contact.Name
                Mobile : contact.Mobile
            }
            , (err) ->
              console.log 'updateMembersNameMobile', err if err
              cbQueue err
        }
    dbq.push {
      run: (cbQueue) ->
        console.log 'update members name and mobile done', --dbq.___count
        cbQueue()
    }

updateVisitorIds = (done)->
  console.log 'begin update visitor ids'
  dbq.___count++

  async.auto {
    openIds : (cb) ->
      db2.tb_module_scrm_member.find {}, { fields : { _id: 0, OpenId : 1 } }
      .toArray (err, docs) ->
        cb err, docs.map((d) -> d.OpenId)
    member : ['openIds', (cb, rst) ->
      db.tb_module_member.aggregate [
        { $unwind : '$OAuth' }
        { $unwind : '$PiwikVisitorID' }
        {
          $match :
            PiwikVisitorID : $exists : true
            'OAuth.OpenID' : $in : rst.openIds
        }
        {
          $group :
            _id : '$OAuth.OpenID'
            PiwikVisitorIDs : $addToSet : '$PiwikVisitorID'
        }
      ], (err, docs) ->
        console.log 'aggregate updateVisitorIds done', err
        console.log 'openIds', rst.openIds.length, 'members', docs.length

        docs.forEach (doc) ->
          dbq.push {
            run: (cbQueue) ->
              db2.tb_module_scrm_member.update
                OpenId : doc._id
              ,
                $addToSet :
                  PiwikVisitorIDs : $each : doc.PiwikVisitorIDs
              ,
                upsert : true
                multi  : true
              , (err) ->
                console.log 'updateVisitorIds member', err if err
                cbQueue err
          }
        dbq.push {
          run: (cbQueue) ->
            cb()
            cbQueue()
        }
    ]
    guest  : ['openIds', (cb, rst) ->
      db.tb_module_oauth_guest.find
        'RawData.openid' : $in : rst.openIds
      .toArray (err, docs) ->
        console.log 'openIds', rst.openIds.length, 'guests', docs.length

        docs.forEach (doc) ->
          dbq.push {
            run: (cbQueue) ->
              db2.tb_module_scrm_member.update
                OpenId : doc.RawData.openid
              ,
                $addToSet :
                  PiwikVisitorIDs : $each : doc.PiwikVisitorID or []
              ,
                upsert : true
                multi  : true
              , (err) ->
                console.log 'updateVisitorIds guest', err if err
                cbQueue err
          }
        dbq.push {
          run: (cbQueue) ->
            cb()
            cbQueue()
        }
    ]
  }, done

tmpLogs = (time, cb) ->
  console.log 'begin tmp_analysis_logs'
  dbq.___count++

  db2.tmp_analysis_logs.remove ->
    db2.tb_module_scrm_logs.aggregate [
      { $match: $and: [
          { AddTime : $gt: time } 
          # { Type    : $ne: 'piwik' }
      ]}
      { $sort: AddTime: 1 }
      {
        $group:
          _id: 
            OpenId: '$OpenId'
            TID   : '$TID'
          offline: $sum: $cond: [
            $or: [
              { $eq: [ '$Type', '摇一摇' ] }
              { $eq: [ '$Type', '星光大道' ] }
              { $eq: [ '$Type', '微信机签到' ] }
            ], 1, 0]
          online: $sum: $cond: [
            $and: [
              { $ne: [ '$Type', '摇一摇' ] }
              { $ne: [ '$Type', '星光大道' ] }
              { $ne: [ '$Type', '微信机签到' ] }
              { $ne: [ '$Type', 'piwik' ] }
            ], 1, 0]
          lastActiveTime : $last: '$AddTime'
      }
      # {
      #   $match:
      #     $or: [
      #       { offline: $gt: 0 }
      #       { online: $gt: 0 }
      #     ]
      # }
      { $out: 'tmp_analysis_logs' }
    ], cb



db.authenticate 'appmae', 'Aim123789', (err, rst) ->
  console.log 'auth', rst

  db2.tb_module_scrm_logs.findOne {}, {
    sort  : AddTime : -1
    limit : 1
  }, (err, log) ->
    return console.error err if err

    beginTime = new Date
    time      = log and log.AddTime or 0
    curTime   = +new Date/1000

    console.log 'lasttime', time

    async.auto [
      (cb) -> activity.run time, curTime, cb
      (cb) -> boat.run time, curTime, cb
      (cb) -> fishing.run time, curTime, cb
      (cb) -> invitative.run time, curTime, cb
      (cb) -> plant.run time, curTime, cb
      (cb) -> shake.run time, curTime, cb
      (cb) -> signin.run time, curTime, cb
      (cb) -> survey.run time, curTime, cb
      (cb) -> ugcgroup.run time, curTime, cb
      (cb) -> tvCheckIn.run time, curTime, cb
      (cb) -> tap.run time, curTime, cb
    ], (err) ->
      require('./openId2')(db, db2, toObjectId, dbq).run time, curTime, (err) ->
        console.log err

      updateMembersNameMobile()

      updateVisitorIds (err) ->
        console.log 'update visitor ids done', err, --dbq.___count

        require('./piwik')(db, db2, toObjectId, dbq).run time, curTime, ->
          tmpLogs time, (err) ->
            console.log 'tmpLogs', err if err

            db2.tmp_analysis_logs.find({}).toArray (err, docs) ->
              docs.forEach (doc) ->
                dbq.push {
                  run: (cbQueue) ->
                    db2.tb_module_scrm_member.update { OpenId: doc._id.OpenId, TID: doc._id.TID }
                    , { $inc: { Offline: doc.offline, Online: doc.online }, $set: LastActiveTime: doc.lastActiveTime }
                    , (err) ->
                      console.log 'tmpLogs member update', err if err
                      cbQueue err
                }
              dbq.push {
                run: (cbQueue) ->
                  console.log 'update count and lastActiveTime done', --dbq.___count
                  cbQueue()
              }

            dbq.drain = ->
              if dbq.___count is 0
                console.log 'all done', new Date - beginTime, err
                process.exit()
