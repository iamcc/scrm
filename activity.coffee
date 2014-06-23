async = require 'async'

module.exports = (db, db2, toObjectId) ->
	db.bind "tb_module_oauth_member"
	db.bind "tb_module_activity_awards_log"
	db.bind "tb_module_activity_data"
	db.bind "tb_module_member"
	db.bind 'tb_module_oauth_guest'

	viewTypes =
        0 : '刮刮卡'
        1 : '大转盘'
        2 : '老虎机'
        3 : '大吉大利'
        4 : '幸福满袋'
        6 : '摇钱树'

	{
		run : (time, callback) ->
			_time = new Date

			console.log 'begin activity'

			getActivityType = (cb, rst) ->
				db.tb_module_activity_data.find {}
					, fields : ViewType : 1
				.toArray (err, docs) ->
					types = {}

					docs.forEach (type) -> types[type._id] = viewTypes[type.ViewType]

					cb err, types

			getMembers = (cb, rst)->
				db.tb_module_activity_awards_log.aggregate [
					{ $match : AddTime : $gt : time }
					{ $group :
							_id      : '$MemberID'
							Contacts : $addToSet : { Name : '$Contact', Mobile : '$Mobile' }
							TIDs     : $addToSet : '$TID'
							Tags		 : $addToSet : '$ActivityID'
							LogIds   : $push : '$_id'
					}
				], (err, docs) ->
					members = {}

					docs.forEach (m) ->
						members[m._id] =
							MemberID : m._id
							Contacts : m.Contacts.filter (mm) -> mm.Name
							TIDs     : m.TIDs
							Tags     : m.Tags.map (tag) -> rst.types[tag]
							LogIds   : m.LogIds

					cb err, members

			updateOpenids = (cb, rst) ->
				ids = (m.MemberID for k, m of rst.members)

				async.auto [
					(cb2) ->
						db.tb_module_member.aggregate [
							{ $match : _id : $in : ids }
							{ $unwind : '$OAuth' }
							{ 
								$group :
									_id       : '$OAuth.OpenID'
									MemberIDs : $addToSet : '$_id'
									NickName : $last : '$NickName'
							}
						], (err, docs) ->
							docs = docs.map (m) ->
								contacts = []
								tIds     = []
								tags     = []
								logIds   = []

								m.MemberIDs.forEach (mid) ->
									member = rst.members[mid]
									member.Contacts.forEach (c) -> contacts.push c
									member.TIDs.forEach (tid) -> tIds.push tid
									member.Tags.forEach (tag) -> tags.push tag
									member.LogIds.forEach (lid) -> logIds.push lid

								{
									OpenId   : m._id
									NickName : m.NickName
									Contacts : contacts
									TIDs     : tIds
									Tags     : tags
									LogIds   : logIds
								}
							cb2 err, docs
					(cb2) ->
						db.tb_module_oauth_member.find
							MemberID : $in : ids
						.toArray (err, docs) ->
							docs = docs.map (member) ->
								{
									OpenId   : member.WxOpenID
									NickName : member.RawData and member.RawData.nickname or ''
									Contacts : rst.members[member.MemberID].Contacts
									TIDs     : rst.members[member.MemberID].TIDs
									Tags     : rst.members[member.MemberID].Tags
									LogIds   : rst.members[member.MemberID].LogIds
								}
							cb2 err, docs
				], (err, rst2) ->
					rst.members = rst2[0].concat rst2[1]
					cb err

			updateLogs = (cb, rst) ->
				async.each rst.members, (member, cbEach) ->
					db.tb_module_activity_awards_log.find
						_id : $in : member.LogIds
					.toArray (err, logs) ->
						logs = logs.map (log) ->
							{
								OpenId     : member.OpenId
								AwardsId   : log.AwardsID
								Mobile     : log.Mobile
								Name       : log.Contact
								AddTime    : log.AddTime
								TID        : log.TID
								DataId 		 : log.ActivityID
								Type 			 : rst.types[log.ActivityID]
							}

						db2.logs.insert logs, cbEach
				, (err) ->
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
							Tags		 : $each : member.Tags
					, upsert : true
					, cbEach
				, cb


			async.auto {
				types 				: getActivityType
				members       : ['types', getMembers]
				openids       : ['members', updateOpenids]
				updateMembers : ['openids', updateMembers]
				logs          : ['openids', 'types', updateLogs]
			}, (err) ->
				console.log 'end activity', new Date - _time
				callback err
	}