async = require 'async'

module.exports = (db, db2, toObjectId, dbq) ->
	db.bind "tb_module_oauth_member"
	db.bind "tb_module_activity_awards_log"
	db.bind "tb_module_activity_data"
	db.bind "tb_module_member"
	db.bind 'tb_module_oauth_guest'

	__MODULE = '营销活动'

	viewTypes =
				0  : '刮刮卡'
				1  : '大转盘'
				2  : '老虎机'
				3  : '大吉大利'
				4  : '幸福满袋'
				6  : '摇钱树'
				7  : '赌大小'
				8  : '捞金鱼'
				9  : '求爱大作战'
				10 : '扭扭蛋'
				5  : '求爱大作战七夕版'

	{
		run : (time, curTime, callback) ->
			_time = new Date

			console.log __MODULE, 'begin'

			getActivityType = (cb, rst) ->
				console.log __MODULE, 'getActivityType'

				db.tb_module_activity_data.find {}
					, fields : ViewType : 1
				.toArray (err, docs) ->
					types = {}

					docs.forEach (type) -> types[type._id] = viewTypes[type.ViewType]

					cb err, types

			getMembers = (cb, rst)->
				console.log __MODULE, 'getMembers'

				db.tb_module_activity_awards_log.aggregate [
					{ $match : $and: [{AddTime: $gt : time}, {AddTime: $lt: curTime}] }
					{ $group :
							_id      :
								MemberID : '$MemberID'
								TID 		 : '$TID'
							Contacts : $addToSet : { Name : '$Contact', Mobile : '$Mobile' }
							Tags		 : $addToSet : '$ActivityID'
							LogIds   : $push : '$_id'
					}
					{ $out: 'tmp_activity_member' }
				], (err) ->
					db.bind 'tmp_activity_member'

					db.tmp_activity_member.find({}).toArray (err, docs) ->
						members = {}

						docs.forEach (m) ->
							members[JSON.stringify(m._id)] =
								MemberID : m._id.MemberID
								TID      : m._id.TID
								Contacts : m.Contacts.filter (mm) -> mm.Name
								Tags     : m.Tags.map (tag) -> rst.types[tag]
								LogIds   : m.LogIds

						cb err, members

			updateOpenids = (cb, rst) ->
				console.log __MODULE, 'updateOpenids'

				ids = (m.MemberID for k, m of rst.members)

				async.auto [
					(cb2) ->
						db.bind 'tmp_activity_openid'

						db.tmp_activity_openid.remove ->
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
								{ $out: 'tmp_activity_openid' }
							], (err) ->
								db.tmp_activity_openid.find({}).toArray (err, docs) ->
									docs = docs.map (m) ->
										contacts = []
										tIds     = []
										tags     = []
										logIds   = []

										m.MemberIDs.forEach (mid) ->
											member = rst.members[JSON.stringify({MemberID: mid, TID: m._id.TID})]
											member.Contacts.forEach (c) -> contacts.push c
											member.Tags.forEach (tag) -> tags.push tag
											member.LogIds.forEach (lid) -> logIds.push lid

										{
											OpenId       : m._id.OpenId
											TID          : m._id.TID
											NickName     : m.NickName
											FaceImageUrl : m.FaceImageUrl
											Sex          : m.Sex
											Contacts     : contacts
											Tags         : tags
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
									Tags         : _m.Tags
									LogIds       : _m.LogIds
								}
							cb2 err, docs
				], (err, rst2) ->
					rst.members = rst2[1]

					oids = {}
					rst2[1].forEach (m) -> oids[m.OpenId] = 1

					for member in rst2[0]
						unless oids[member.OpenId]
							rst.members.push member
					cb err

			updateLogs = (cb, rst) ->
				console.log __MODULE, 'updateLogs'

				rst.members.forEach (member) ->
					if member.LogIds
						dbq.push {
							run: (cbQueue) ->
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

									async.each logs, (log, cbEach) ->
										db2.tb_module_scrm_logs.insert log, (err) ->
											console.error __MODULE, 'updateLogs', err if err
											cbEach()
									, cbQueue


									# db2.tb_module_scrm_logs.insert logs, (err) ->
									# 	console.log __MODULE, 'updateLogs', err if err
									# 	cbQueue err
						}
				cb()

			updateMembers = (cb, rst) ->
				console.log __MODULE, 'updateMembers'
				
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
									Contacts : $each : member.Contacts or []
									Tags		 : $each : member.Tags or []
							, upsert : true
							, (err) ->
								console.log __MODULE, 'updateMembers', err if err
								cbQueue err
							}
				cb()

			async.auto {
				types 				: getActivityType
				members       : ['types', getMembers]
				openids       : ['members', updateOpenids]
				updateMembers : ['openids', updateMembers]
				logs          : ['openids', 'types', updateLogs]
			}, (err) ->
				callback err if err

				dbq.push {
					run: (cbQueue) ->
						callback()
						cbQueue();
						console.log 'end', __MODULE, new Date - _time, err
				}
	}