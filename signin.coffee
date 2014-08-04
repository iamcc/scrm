async = require 'async'

module.exports = (db, db2, toObjectId, dbq) ->
	db.bind 'tb_module_signin_member'

	__type = '星光大道'

	{
		run : (time, curTime, callback) ->
			_time = new Date

			console.log __type, 'begin'

			getMembers = (cb) ->
				console.log __type, 'getMembers'

				db.tb_module_signin_member.aggregate [
					{ $match : $and: [{AddTime: $gt : time}, {AddTime: $lt: curTime}] }
					{
						$group :
							_id       : 
								GuestId : '$MemberID'
								TID     : '$TID'
							Contacts  : $addToSet : { Name : '$Name', Mobile : '$Mobile' }
							LogIds    : $push : '$_id'
					}
				], (err, docs) ->
					members = {}

					docs.forEach (member) ->
						members[JSON.stringify(member._id)] =
							GuestId  : member._id.GuestId
							TID      : member._id.TID
							Contacts : member.Contacts.filter (c) -> c.Name
							LogIds   : member.LogIds

					cb err, members

			updateOpenIds = (cb, rst) ->
				console.log __type, 'updateOpenIds'

				ids = (m.GuestId for k, m of rst.members)

				async.auto [
					(cb2) ->
						db.tb_module_member.aggregate [
							{ $match : _id : $in : ids }
							{ $unwind : '$OAuth' }
							{ 
								$group :
									_id       : 
										OpenId : '$OAuth.OpenID'
										TID    : '$TID'
									MemberIDs    : $addToSet : '$_id'
									NickName     : $last : '$NickName'
									FaceImageUrl : $last : '$FaceImageUrl'
									Sex          : $last : '$Sex'
							}
						], (err, docs) ->
							docs = docs.map (m) ->
								contacts = []
								tIds     = []
								logIds   = []

								m.MemberIDs.forEach (mid) ->
									member = rst.members[JSON.stringify({GuestId: mid, TID: m._id.TID})]
									member.Contacts.forEach (c) -> contacts.push c
									member.LogIds.forEach (lid) -> logIds.push lid

								{
									OpenId       : m._id.OpenId
									TID          : m._id.TID
									NickName     : m.NickName
									FaceImageUrl : m.FaceImageUrl
									Sex          : m.Sex
									Contacts     : contacts
									LogIds       : logIds
								}

							cb2 err, docs
					(cb2) ->
						db.tb_module_oauth_member.find
							MemberID : $in : ids
						.toArray (err, docs) ->
							docs = docs.map (member) ->
								_m = rst.members[JSON.stringify({MemberID: member.MemberID, TID: member.TID})] or {}

								{
									OpenId       : member.WxOpenID
									TID          : member.TID
									NickName     : member.RawData and member.RawData.nickname or ''
									FaceImageUrl : member.RawData and member.RawData.headimgurl or ''
									Sex          : member.RawData and member.RawData.sex or ''
									Contacts     : _m.Contacts
									LogIds       : _m.LogIds
								}
							cb2 err, docs
				], (err, rst2) ->
					rst.members = rst2[0].concat rst2[1]
					cb err

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
								$addToSet :
									Contacts : $each : member.Contacts or []
									Tags		 : __type
							, upsert : true
							, (err) ->
								console.log __type, 'updateMembers', err if err
								cbQueue err
							}
				cb()

			updateLogs = (cb, rst) ->
				console.log __type, 'updateLogs'

				rst.members.forEach (member) ->
					if member.LogIds
						dbq.push {
							run: (cbQueue) ->
								db.tb_module_signin_member.find
									_id : $in : member.LogIds
								.toArray (err, logs) ->
									logs = logs.map (log) ->
										{
											OpenId  : member.OpenId
											Mobile  : log.Mobile
											Name    : log.Name
											AddTime : log.AddTime
											TID     : log.TID
											DataId  : log.SigninID
											Type    : __type
										}

									async.each logs, (log, cbEach) ->
										db2.tb_module_scrm_logs.insert log, (err) ->
											console.log __type, 'updateLogs', err if err
											cbEach()
									, cbQueue
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