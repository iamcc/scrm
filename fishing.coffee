async = require 'async'

module.exports = (db, db2, toObjectId) ->
	db.bind 'tb_module_fishing_awards_log'
	db.bind 'tb_module_fishing_define'
	db.bind 'tb_module_fishing_guest'

	__type = '垂钓王'

	{
		run : (time, callback) ->
			_time = new Date

			console.log 'begin fishing'

			getMembers = (cb) ->
				db.tb_module_fishing_guest.aggregate [
					{ $match : AddTime : $gt : time }
					# { $sort : AddTime : 1 }
					{
						$group :
							_id       : '$GuestID'
							TIDs      : $addToSet : '$TID'
							Contacts  : $addToSet : { Name : '$Name', Mobile : '$Mobile' }
							# LastActiveTime : $last: '$AddTime'
					}
				], (err, docs) ->
					members = {}

					async.each docs, (member, cbEach) ->
						db.tb_module_fishing_awards_log.find
							AddTime : $gt : time
							GuestID : member._id
						,
							fields : _id : 1
						.toArray (err, logs) ->
							members[member._id] =
								GuestId  : member._id
								TIDs     : member.TIDs
								Contacts : member.Contacts.filter (c) -> c.Name
								LogIds   : logs.map (log) -> log._id
								# LastActiveTime : member.LastActiveTime
							cbEach err
					, (err) ->
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

						if logs.length then db2.logs.insert logs, cbEach
						else cbEach null
				, cb

			async.auto {
				members       : getMembers
				updateOpenIds : ['members', updateOpenIds]
				updateMembers : ['updateOpenIds', updateMembers]
				updateLogs    : ['updateOpenIds', updateLogs]
			}, (err) ->
				console.log 'end fishing', new Date - _time
				callback err 
	}