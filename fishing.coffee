async = require 'async'

module.exports = (db, db2, toObjectId, dbq) ->
	db.bind 'tb_module_fishing_awards_log'
	db.bind 'tb_module_fishing_define'
	db.bind 'tb_module_fishing_guest'

	__type   = '垂钓王'

	{
		run : (time, curTime, callback) ->
			_time = new Date

			console.log __type, 'begin'

			getMembers = (cb) ->
				console.log __type, 'getMembers'

				db.tb_module_fishing_guest.aggregate [
					{ $match : $and: [{AddTime: $gt : time}, {AddTime: $lt: curTime}] }
					{
						$group :
							_id       : 
								GuestId : '$GuestID'
								TID     : '$TID'
							Contacts  : $addToSet : { Name : '$Name', Mobile : '$Mobile' }
					}
				], (err, docs) ->
					members = {}

					async.each docs, (member, cbEach) ->
						db.tb_module_fishing_awards_log.find
							$and: [{AddTime: $gt : time}, {AddTime: $lt: curTime}]
							GuestID : member._id.GuestId
							TID     : member._id.TID
						,
							fields : _id : 1
						.toArray (err, logs) ->
							members[JSON.stringify(member._id)] =
								GuestId  : member._id.GuestId
								TID      : member._id.TID
								Contacts : member.Contacts.filter (c) -> c.Name
								LogIds   : logs.map (log) -> log._id
							cbEach err
					, (err) ->
						cb err, members

			updateOpenIds = (cb, rst) ->
				console.log __type, 'updateOpenIds'

				members = rst.members

				db.tb_module_oauth_guest.find
					_id : $in : (m.GuestId for k, m of members)
				.toArray (err, docs) ->
					guestOpenId = {}

					docs.forEach (member) ->
						guestOpenId[member._id] =
							OpenId       : member.WxOpenID
							NickName     : member.RawData and member.RawData.nickname or ''
							FaceImageUrl : member.RawData and member.RawData.headimgurl or ''
							Sex          : member.RawData and member.RawData.sex or ''

					for k, member of members
						member.OpenId       = guestOpenId[member.GuestId].OpenId
						member.NickName     = guestOpenId[member.GuestId].NickName
						member.FaceImageUrl = guestOpenId[member.GuestId].FaceImageUrl
						member.Sex          = guestOpenId[member.GuestId].Sex

					rst.members = (m for k, m of members)

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
					if member.LogIds and member.LogIds.length
						dbq.push {
							run: (cbQueue) ->
								db.tb_module_fishing_awards_log.find
									_id : $in : member.LogIds
								.toArray (err, logs) ->
									logs = logs.map (log) ->
										{
											OpenId     : member.OpenId
											Mobile     : log.Mobile
											Name       : log.Name
											AddTime    : log.AddTime
											TID        : log.TID
											DataId 		 : log.FishingID
											Type 			 : __type
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