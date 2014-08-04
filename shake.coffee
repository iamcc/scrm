async = require 'async'

###
db.tb_module_shake_data.find().forEach(function(doc){
  db.tb_module_shake_data.update({_id: doc._id}, {$set:{AddTime: doc._id.getTimestamp().getTime()/1000}});
});
###

module.exports = (db, db2, toObjectId, dbq) ->
	db.bind 'tb_module_shake_data'

	__type = '摇一摇'

	{
		run : (time, curTime, callback) ->
			_time = new Date

			console.log __type, 'begin'

			getMembers = (cb) ->
				console.log __type, 'getMembers'

				db.tb_module_shake_data.aggregate [
					{ $match : $and: [{AddTime: $gt : time}, {AddTime: $lt: curTime}] }
					{
						$group :
							_id       : 
								GuestId : '$MemberID'
								TID      : '$TID'
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

				db.tb_module_member.aggregate [
					{ $match : _id : $in : (m.GuestId for k, m of rst.members) }
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
					rst.members = docs.map (m) ->
						contacts = []
						tIds     = []
						logIds   = []

						m.MemberIDs.forEach (mid) ->
							member = rst.members[JSON.stringify({GuestId: mid, TID: m._id.TID})] or {}
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
								db.tb_module_shake_data.find
									_id : $in : member.LogIds
								.toArray (err, logs) ->
									logs = logs.map (log) ->
										{
											OpenId  : member.OpenId
											Mobile  : log.Mobile
											Name    : log.Name
											AddTime : log.AddTime
											TID     : log.TID
											DataId  : log.ShakeID
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