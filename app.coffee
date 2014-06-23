async      = require 'async'
mongo      = require 'mongoskin'
toObjectId = mongo.helper.toObjectID
db         = mongo.db "mongodb://localhost/app_mae", native_parser: true
db2        = mongo.db "mongodb://localhost/app_mae_analysis", native_parser: true

db.bind 'tb_module_member'
db.bind 'tb_module_oauth_guest'

db2.bind 'members'
db2.bind 'logs'

# 营销活动
activity = require('./activity') db, db2, toObjectId

# 爬龙舟
boat = require('./boat') db, db2, toObjectId

# 垂钓网
fishing = require('./fishing') db, db2, toObjectId

# 邀请函
invitative = require('./invitative') db, db2, toObjectId

# 公益树
plant = require('./plant') db, db2, toObjectId

# 摇一摇
shake = require('./shake') db, db2, toObjectId

# 星光大道
signin = require('./signin') db, db2, toObjectId

# 有奖调查
survey = require('./survey') db, db2, toObjectId

# 社会化拼团
ugcgroup = require('./ugcgroup') db, db2, toObjectId

updateMembersNameMobile = ->
  console.log 'begin update members name and mobile'

  db2.members.find { Name: $exists: false }
  .toArray (err, members) ->
    async.each members, (member, callback) ->
      if member.Contacts.length
        contact = member.Contacts.slice(-1)[0]
        db2.members.update { _id: member._id }
        , {
          $set:
            Name: contact.Name
            Mobile: contact.Mobile
        }, callback
      else
        callback()
    , (err) ->
      console.log 'update members name and mobile done', err

updateVisitorIds = (done)->
  console.log 'begin update visitor ids'

  async.auto {
    openIds : (cb) ->
      db2.members.find {}, { fields : { _id: 0, OpenId : 1 } }
      .toArray (err, docs) ->
        cb err, docs.map (d) -> d.OpenId
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
        async.each docs, (doc, cbEach) ->
          db2.members.update
            OpenId : doc._id
          ,
            $addToSet :
              PiwikVisitorIDs : $each : doc.PiwikVisitorIDs
          ,
            upsert : true
          , cbEach
        , cb
    ]
    guest  : ['openIds', (cb, rst) ->
      db.tb_module_oauth_guest.find
        _id : $in : rst.openIds
      .toArray (err, docs) ->
        async.each docs, (doc, cbEach) ->
          db2.members.update
            OpenId : doc._id
          ,
            $addToSet :
              PiwikVisitorIDs : $each : doc.PiwikVisitorID
          ,
            upsert : true
          , cbEach
        , cb
    ]
  }, done

updateCount = (time) ->
  console.log 'begin update count'

  db2.logs.aggregate [
    { $match: $and: [
        { AddTime : $gt: time } 
        { Type    : $ne: 'piwik' }
    ]}
    {
      $group:
        _id: '$OpenId'
        offline: $sum: $cond: [
          $or: [
            { $eq: [ '$Type', '摇一摇' ] }
            { $eq: [ '$Type', '星光大道' ] }
          ], 1, 0]
        online: $sum: $cond: [
          $and: [
            { $ne: [ '$Type', '摇一摇' ] }
            { $ne: [ '$Type', '星光大道' ] }
          ], 1, 0]
    }
    {
      $match:
        $or: [
          { offline: $gt: 0 }
          { online: $gt: 0 }
        ]
    }
  ], (err, docs) ->
    return console.log err if err

    docs.forEach (doc) ->
      db2.members.update { OpenId: doc._id }
      , { $inc: { Offline: doc.offline, Online: doc.online } }
      , ->

    console.log 'update count done'

updateLastActiveTime = (time) ->
  console.log 'begin update last active time'

  db2.logs.aggregate [
    {
      $match:
        $and: [
          { AddTime : $gt: time }
          { Type    : $ne: 'piwik' }
        ]
    }
    { $sort: AddTime: 1 }
    {
      $group:
        _id            : '$OpenId'
        lastActiveTime : $last: '$AddTime'
    }
  ], (err, docs) ->
    return console.error err if err

    async.each docs, (doc, cbEach) ->
      db2.members.update { OpenId: doc._id }, { $set: LastActiveTime: doc.lastActiveTime }, cbEach
    , (err) ->
      console.log 'update last active time done', err

db2.logs.findOne {}, {
  sort  : AddTime : -1
  limit : 1
}, (err, log) ->
  return console.error err if err

  beginTime = new Date
  time      = log and log.AddTime or 0

  console.log 'lasttime', time

  async.auto [
    (cb) -> activity.run time, cb
    (cb) -> boat.run time, cb
    (cb) -> fishing.run time, cb
    (cb) -> invitative.run time, cb
    (cb) -> plant.run time, cb
    (cb) -> shake.run time, cb
    (cb) -> signin.run time, cb
    (cb) -> survey.run time, cb
    (cb) -> ugcgroup.run time, cb
  ], (err) ->
    updateMembersNameMobile()

    updateCount(time)

    updateLastActiveTime(time)

    updateVisitorIds (err, rst) ->
      console.log 'update visitor ids done', new Date - beginTime, err

      require('./piwik')(db2).run time, (err) ->
        console.log 'all done', new Date - beginTime, err

        process.exit()






























