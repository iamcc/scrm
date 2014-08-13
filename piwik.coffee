async   = require 'async'
mysql   = require 'mysql'
moment  = require 'moment'

pool    = mysql.createPool {
	connectionLimit : 10
	host            : '10.221.152.154'
	# host            : 'localhost'
	user            : 'analytics'
	# password        : ''
	password        : 'aim123789'
	database				: 'piwik'
}

module.exports = (db, db2, toObjectId, dbq) ->
	{
		run: (time, curTime, callback) ->
			dbq.___count++
			beginTime = new Date
			console.log 'begin update piwik'

			getAllSites = (cb, rst) ->
				pool.query 'select idsite, name, tid from piwik_site', (err, rows) ->
					console.log 'piwik getAllSites', rows and rows.length
					cb err, rows

			getAllVisitorIds = (cb, rst) ->
				visitorIds = {}

				db2.tb_module_scrm_member.find {}
				.toArray (err, members) ->
					members.forEach (member) ->
						if member.PiwikVisitorIDs
							member.PiwikVisitorIDs.forEach (vid) ->
								visitorIds[vid] = member.OpenId

					console.log 'piwik getAllVisitorIds', Object.keys(visitorIds).length
					cb err, visitorIds

			getVisitsDetail = (cb, rst) ->
				time2    = moment(time*1000).format 'YYYY-MM-DD HH:mm:ss'
				curTime2 = moment(curTime*1000).format 'YYYY-MM-DD HH:mm:ss'

				rst.sites.forEach (site) ->
					dbq.push {
						run: (cbQueue) ->
							pool.query 'select
														idvisit, idvisitor, visit_last_action_time
														, location_country, location_city, location_latitude, location_longitude, location_ip, config_os
													from
														piwik_log_visit
													where
														visit_last_action_time > ? and visit_last_action_time < ? and idsite = ?'
							, [time2, curTime2, site.idsite]
							, (err, visits) ->
								toSave = []

								async.each visits, (visit, cbEach) ->
									unless openId = rst.visitorIds[visit.idvisitor.toString('hex')]
										return cbEach()

									pool.query 'select
																idsite, idvisitor, server_time, idvisit, time_spent_ref_action, la.name, la2.name as url
															from
																piwik_log_link_visit_action va
																join piwik_log_action la on va.idaction_name = la.idaction
																join piwik_log_action la2 on va.idaction_url = la2.idaction
															where
																idvisit = ?'
									, [visit.idvisit]
									, (err, actions) ->
										visit = {
											OpenId    : openId
											TID       : toObjectId(site.tid)
											AddTime   : +new Date(visit.visit_last_action_time)/1000
											Type      : 'piwik'
											PiwikData :
												Name      : site.name
												VisitorId : visit.idvisitor.toString('hex')
												Country   : visit.location_country
												City      : visit.location_city
												Longitude : visit.location_longitude
												Latitude  : visit.location_latitude
												IP        : visit.location_ip.toString('hex')
												OS        : visit.config_os
												Details   :
													actions.map (action) ->
														Title     : action.name
														Url       : action.url
														AddTime   : +new Date(action.server_time)/1000
														SpentTime : if action.time_spent_ref_action is 0 then 3 else action.time_spent_ref_action
										}
										toSave.push visit
										cbEach err
								, (err) ->
									# cnt2 += toSave.length

									async.each toSave, (log, cbEach) ->
										db2.tb_module_scrm_logs.insert log, (err, inserted) ->
											console.error err if err
											# cnt += inserted and inserted.length or 0
											cbEach()
									, (err) ->
										# console.log cnt, cnt2
										cbQueue()

									# if toSave.length then db2.tb_module_scrm_logs.insert toSave, (err, inserted) ->
									# 	cbQueue (err)
									# 	cnt += inserted and inserted.length or 0
									# 	console.log err if err
									# 	console.log cnt, cnt2
									# else cbQueue err
					}
				dbq.push {
					run: (cbQueue) ->
						db2.tb_module_scrm_logs.aggregate [
							{ 
								$match: 
									PiwikData : $exists: true
									AddTime   : $gt: time
							}
							{ $unwind: '$PiwikData.Details' }
							{
								$group :
									_id :
										OpenId : '$OpenId'
										TID    : '$TID'
										Title  : '$PiwikData.Details.Title'
									Time  : $max : '$PiwikData.Details.SpentTime'
							}
							{ $project: { TID: '$_id.TID', OpenId: '$_id.OpenId', Title: '$_id.Title', Time: 1 } }
							{$out: 'tmp_pages'}
						], (err) ->
							console.error err if err
							db2.bind 'tmp_pages'

							console.log 'begin tmp_pages'

							db2.tmp_pages.aggregate [
								{
									$group :
										_id :
											TID : '$TID'
											OpenId : '$OpenId'
								}
							], (err, ids) ->
								async.eachLimit ids, 10, (id, cbEach) ->
									db2.tmp_pages.find(id._id).toArray (err, docs) ->
										pages = docs.map (page) ->
											return {
												Title: page.Title
												Time: page.Time
											}

										db2.tb_module_scrm_member.find(id._id).toArray (err, member) ->
											if member.length == 0 then return cbEach err
											member = member[0]
											member.Pages = member.Pages or []
											newPages = []

											for page in pages
												isFind = false
												for mpage in member.Pages
													if page.Title is mpage.Title
														isFind = true
														if page.Time > mpage.Time
															mpage.Time = page.Time
												if not isFind
													newPages.push page
											member.Pages = member.Pages.concat newPages

											db2.tb_module_scrm_member.save member, (err, cnt) ->
												console.log 'save pages', err, cnt
												cbEach err
								, cbQueue
				}
				dbq.push {
					run: (cbQueue) ->
						dbq.___count--
						cb()
						cbQueue()
				}

			async.auto {
				sites      : getAllSites
				visitorIds : getAllVisitorIds
				details    : ['sites', 'visitorIds', getVisitsDetail]
			}, (err) ->
				console.log 'update piwik done', new Date - beginTime, err
				callback err
	}