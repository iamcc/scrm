async   = require 'async'
mysql   = require 'mysql'

pool    = mysql.createPool {
	connectionLimit : 10
	host            : 'localhost'
	user            : 'root'
	password        : ''
	database				: 'piwik'
}

module.exports = (db) ->
	{
		run: (time, callback) ->
			beginTime = new Date
			console.log 'begin update piwik'

			getAllSites = (cb, rst) ->
				pool.query 'select idsite, name, tid from piwik_site', (err, rows) ->
					cb err, rows

			getAllVisitorIds = (cb, rst) ->
				visitorIds = {}

				db.members.find {}
				.toArray (err, members) ->
					members.forEach (member) ->
						if member.PiwikVisitorIDs
							member.PiwikVisitorIDs.forEach (vid) ->
								visitorIds[vid] = member.OpenId

					cb err, visitorIds

			getVisitsDetail = (cb, rst) ->
				date = if time == 0 then new Date(2013, 0, 1) else new Date time*1000
				date = date.getFullYear() + '-' + (date.getMonth()+1) + '-' + date.getDate()

				visitsToSave = []

				t = setInterval ->
					db.logs.insert visitsToSave.splice(0), (err, inserted) ->
						# console.log err, inserted.length
				, 1000

				q = async.queue (task, callback) ->
					task.run callback
				, 10

				q.drain = ->
					clearInterval t
					visits = visitsToSave.splice(0)
					if visits.length then db.logs.insert visits, (err, inserted) ->
						# console.log err, inserted.length, 'last'

					cb()

				rst.sites.forEach (site) ->
					q.push {
						run  : (callback) ->
							pool.query 'select
														idvisit, idvisitor, visit_last_action_time
													from
														piwik_log_visit
													where
														visit_last_action_time > ? and idsite = ?'
							, [date, site.idsite]
							, (err, visits) ->
								process.nextTick ->
									visits.forEach (visit) ->
										return unless openId = rst.visitorIds[visit.idvisitor.toString('hex')]

										q.unshift {
											run: (callback2) ->
												pool.query 'select
																			idsite, idvisitor, server_time, idvisit, name, time_spent_ref_action
																		from
																			piwik_log_link_visit_action va join piwik_log_action la
																		where
																			va.idaction_name = la.idaction
																			and idvisit = ?'
												, [visit.idvisit]
												, (err, actions) ->
													visit = {
														OpenId    : openId
														TID       : site.tid
														AddTime   : +new Date(visit.visit_last_action_time)/1000
														Type      : 'piwik'
														PiwikData :
															Name      : site.name
															VisitorId : visit.idvisitor.toString('hex')
															Details   :
																actions.map (action) ->
																	Title     : action.name
																	AddTime   : +new Date(action.server_time)/1000
																	SpentTime : action.time_spent_ref_action
													}

													visitsToSave.push visit

													callback2()
										}
									callback()
					}

			async.auto {
				sites: getAllSites
				visitorIds: getAllVisitorIds
				details: ['sites', 'visitorIds', getVisitsDetail]
			}, (err) ->
				console.log 'update piwik done', new Date - beginTime
				callback err
	}


























