async = require 'async'

module.exports = (db, db2, toObjectId, dbq) ->
  db.bind 'tb_module_member'
  db.bind 'tb_module_member_card'

  __type   = '会员卡'

  {
    run : (time, curTime, callback) ->
      _time = new Date

      console.log __type, 'begin'

      getMembers = (cb) ->
        console.log __type, 'getMembers'

        db.tb_module_member_card.find {
          AddTime:
            $gt: time
            $lt: curTime
        }
        .toArray cb

      updateOpenIds = (cb, rst) ->
        console.log __type, 'updateOpenIds'

        members = rst.members

        async.eachLimit members, 10, (member, cbEach) ->
          db.tb_module_member.findOne {_id: member.MemberID}, (err, doc) ->
            if err
              console.log __type, err
              return cbEach err

            if doc
              if oauth = doc.OAuth and doc.OAuth.length > 0 and doc.OAuth[0]
                member.OpenId       = oauth.OpenID
                member.NickName     = doc.NickName
                member.FaceImageUrl = doc.FaceImageUrl
                member.Sex          = doc.Sex

            cbEach err, doc
        , (err) ->
          console.log err if err
          cb err, members

      updateMembers = (cb, rst) ->
        console.log __type, 'updateMembers'

        rst.members.forEach (member) ->
          dbq.push {
            run: (cbQueue) ->
              db2.tb_module_scrm_member.update
                OpenId : member.OpenId
                TID    : member.TID
              ,
                $set :
                  OpenId       : member.OpenId
                  TID          : member.TID
                  NickName     : member.NickName
                  FaceImageUrl : member.FaceImageUrl
                  Sex          : member.Sex
                $addToSet :
                  Contacts : { Name: member.Name, Mobile: member.Mobile }
                  Tags     : __type
              , upsert : true
              , (err) ->
                console.log __type, 'updateMembers', err if err
                cbQueue err
          }

        cb()

      updateLogs = (cb, rst) ->
        console.log __type, 'updateLogs'

        rst.members.forEach (member) ->
          dbq.push {
            run: (cbQueue) ->
              db2.tb_module_scrm_logs.insert {
                OpenId  : member.OpenId
                Mobile  : member.Mobile
                Name    : member.Name
                AddTime : member.AddTime
                TID     : member.TID
                Type    : __type
              }, (err) ->
                console.log __type, 'updateLogs', err if err
                cbQueue err
          }

        cb()

      async.auto {
        members       : getMembers
        updateOpenIds : ['members', updateOpenIds]
        updateMembers : ['updateOpenIds', updateMembers]
        updateLogs    : ['updateOpenIds', updateLogs]
      }, (err) ->
        callback err if err

        dbq.push {
          run: (cbQueue) ->
            cbQueue()
            callback()
            console.log 'end', __type, new Date - _time, err
        }
  }