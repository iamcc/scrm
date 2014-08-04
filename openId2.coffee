async = require 'async'

module.exports = (db, db2, toObjectId, dbq) ->
  __type   = '更新OpenID2'

  {
    run : (time, curTime, callback) ->
      _time = new Date

      console.log __type, 'begin'

      db.bind 'tb_module_oauth_member'

      db.tb_module_oauth_member.find { AddTime: $gt: time }
      .toArray (err, docs) ->
        async.eachLimit docs, 10, (doc, cbEach) ->
          db2.tb_module_scrm_member.update { TID: doc.TID, OpenId: doc.WxOpenID }, { $set: { OpenId2: doc.RawData.openid } }, (err, doc) ->
            console.log err, doc
            cbEach err
        , (err) ->
          console.log 'done', err
  }