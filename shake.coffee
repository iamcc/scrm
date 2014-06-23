async = require 'async'

###
db.tb_module_shake_data.find().forEach(function(doc){
  db.tb_module_shake_data.update({_id: doc._id}, {$set:{AddTime: doc._id.getTimestamp().getTime()/1000}});
});
###

module.exports = (db, db2, toObjectId) ->
	db.bind 'tb_module_shake_data'

	__type = '摇一摇'

	{
		run : (time, callback) ->
			_time = new Date

			console.log 'begin shake'

			getMembers = (cb) ->
				db.tb_module_shake_data.aggregate [
					{ $match : AddTime : $gt : time }
					{
						$group :
							_id       : '$MemberID'
							TIDs      : $addToSet : '$TID'
							Contacts  : $addToSet : { Name : '$Name', Mobile : '$Mobile' }
							LogIds    : $push : '$_id'
					}
				], (err, docs) ->
					members = {}

					docs.forEach (member) ->
						members[member._id] =
							GuestId  : member._id
							TIDs     : member.TIDs
							Contacts : member.Contacts.filter (c) -> c.Name
							LogIds   : member.LogIds

					cb err, members

			updateOpenIds = (cb, rst) ->
				db.tb_module_member.aggregate [
					{ $match : _id : $in : Object.keys(rst.members).map (id) -> toObjectId(id) }
					{ $unwind : '$OAuth' }
					{ 
						$group :
							_id       : '$OAuth.OpenID'
							MemberIDs : $addToSet : '$_id'
							NickNames : $addToSet : '$NickName'
					}
				], (err, docs) ->
					docs = docs.map (m) ->
						contacts = []
						tIds     = []
						logIds   = []

						m.MemberIDs.forEach (mid) ->
							member = rst.members[mid]
							member.Contacts.forEach (c) -> contacts.push c
							member.TIDs.forEach (tid) -> tIds.push tid
							member.LogIds.forEach (lid) -> logIds.push lid

						{
							OpenId   : m._id
							NickName : m.NickNames[m.NickNames.length - 1]
							Contacts : contacts
							TIDs     : tIds
							LogIds   : logIds
						}

					rst.members = docs

					cb err

			updateMembers = (cb, rst) ->
				async.each rst.members, (member, cbEach) ->
					db2.members.update
						OpenId : member.OpenId
					,
						$set :
							OpenId   : member.OpenId
							NickName : member.NickName
						$addToSet :
							Contacts : $each : member.Contacts
							TIDs     : $each : member.TIDs
							Tags     : __type
					, upsert : true
					, cbEach
				, cb

			updateLogs = (cb, rst) ->
				async.each rst.members, (member, cbEach) ->
					db.tb_module_shake_data.find
						_id : $in : member.LogIds
					.toArray (err, logs) ->
						logs = logs.map (log) ->
							{
								OpenId     : member.OpenId
								Mobile     : log.Mobile
								Name       : log.Name
								AddTime    : log.AddTime
								# AddTime    : log._id.getTimestamp().getTime()/1000
								TID        : log.TID
								DataId 		 : log.ShakeID
								Type 			 : __type
							}
						if logs.length then db2.logs.insert logs, cbEach
						else cbEach()
				, cb

			async.auto {
				members       : getMembers
				updateOpenIds : ['members', updateOpenIds]
				updateMembers : ['updateOpenIds', updateMembers]
				updateLogs    : ['updateOpenIds', updateLogs]
			}, (err) ->
				console.log 'end shake', new Date - _time
				callback err 
	}