async = require 'async'

module.exports = (db, db2, toObjectId) ->
	db.bind 'tb_module_boat_guest'
	db.bind 'tb_module_oauth_guest'

	__type = '赛龙舟'

	{
		run : (time, callback) ->
			_time = new Date

			console.log 'begin boat'

			getMembers = (cb) ->
				db.tb_module_boat_guest.aggregate [
					{ $match : AddTime : $gt : time }
					# { $sort : AddTime : 1 }
					{
						$group :
							_id       : '$GuestID'
							TIDs      : $addToSet : '$TID'
							Contacts  : $addToSet : { Name : '$Name', Mobile : '$Mobile' }
							LogIds    : $push : '$_id'
							# LastActiveTime : $last: '$AddTime'
					}
				], (err, docs) ->
					members = {}

					docs.forEach (member) ->
						members[member._id] =
							GuestId  : member._id
							TIDs     : member.TIDs
							Contacts : member.Contacts.filter (c) -> c.Name
							LogIds   : member.LogIds
							# LastActiveTime : member.LastActiveTime

					cb err, members

			updateOpenIds = (cb, rst) ->
				members = rst.members

				db.tb_module_oauth_guest.find
					_id : $in : (m.GuestId for k, m of members)
				.toArray (err, docs) ->
					rst.members = docs.map (member) ->
						{
							OpenId   : member.WxOpenID
							NickName : member.RawData and member.RawData.nickname or ''
							Contacts : rst.members[member._id].Contacts
							TIDs     : rst.members[member._id].TIDs
							LogIds   : rst.members[member._id].LogIds
							# LastActiveTime : rst.members[member._id].LastActiveTime
						}

					cb err

			updateMembers = (cb, rst) ->
				async.each rst.members, (member, cbEach) ->
					db2.members.update
						OpenId : member.OpenId
					,
						$set :
							OpenId   : member.OpenId
							NickName : member.NickName
							# LastActiveTime : member.LastActiveTime
						$addToSet :
							Contacts : $each : member.Contacts
							TIDs     : $each : member.TIDs
							Tags     : __type
					, upsert : true
					, cbEach
				, cb

			updateLogs = (cb, rst) ->
				async.each rst.members, (member, cbEach) ->
					db.tb_module_boat_guest.find
						_id : $in : member.LogIds
					.toArray (err, logs) ->
						logs = logs.map (log) ->
							{
								OpenId     : member.OpenId
								Mobile     : log.Mobile
								Name       : log.Name
								AddTime    : log.AddTime
								TID        : log.TID
								DataId 		 : log.TeamID
								Type 			 : __type
							}
						db2.logs.insert logs, cbEach
				, cb

			async.auto {
				members       : getMembers
				updateOpenIds : ['members', updateOpenIds]
				updateMembers : ['updateOpenIds', updateMembers]
				updateLogs    : ['updateOpenIds', updateLogs]
			}, (err) ->
				console.log 'end boat', new Date - _time
				callback err 
	}