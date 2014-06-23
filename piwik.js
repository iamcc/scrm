// Generated by CoffeeScript 1.7.1
var async, mysql, pool;

async = require('async');

mysql = require('mysql');

pool = mysql.createPool({
  connectionLimit: 10,
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'piwik'
});

module.exports = function(db) {
  return {
    run: function(time, callback) {
      var beginTime, getAllSites, getAllVisitorIds, getVisitsDetail;
      beginTime = new Date;
      console.log('begin update piwik');
      getAllSites = function(cb, rst) {
        return pool.query('select idsite, name, tid from piwik_site', function(err, rows) {
          return cb(err, rows);
        });
      };
      getAllVisitorIds = function(cb, rst) {
        var visitorIds;
        visitorIds = {};
        return db.members.find({}).toArray(function(err, members) {
          members.forEach(function(member) {
            if (member.PiwikVisitorIDs) {
              return member.PiwikVisitorIDs.forEach(function(vid) {
                return visitorIds[vid] = member.OpenId;
              });
            }
          });
          return cb(err, visitorIds);
        });
      };
      getVisitsDetail = function(cb, rst) {
        var date, q, t, visitsToSave;
        date = time === 0 ? new Date(2013, 0, 1) : new Date(time * 1000);
        date = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate();
        visitsToSave = [];
        t = setInterval(function() {
          return db.logs.insert(visitsToSave.splice(0), function(err, inserted) {});
        }, 1000);
        q = async.queue(function(task, callback) {
          return task.run(callback);
        }, 10);
        q.drain = function() {
          var visits;
          clearInterval(t);
          visits = visitsToSave.splice(0);
          if (visits.length) {
            db.logs.insert(visits, function(err, inserted) {});
          }
          return cb();
        };
        return rst.sites.forEach(function(site) {
          return q.push({
            run: function(callback) {
              return pool.query('select idvisit, idvisitor, visit_last_action_time from piwik_log_visit where visit_last_action_time > ? and idsite = ?', [date, site.idsite], function(err, visits) {
                return process.nextTick(function() {
                  visits.forEach(function(visit) {
                    var openId;
                    if (!(openId = rst.visitorIds[visit.idvisitor.toString('hex')])) {
                      return;
                    }
                    return q.unshift({
                      run: function(callback2) {
                        return pool.query('select idsite, idvisitor, server_time, idvisit, name, time_spent_ref_action from piwik_log_link_visit_action va join piwik_log_action la where va.idaction_name = la.idaction and idvisit = ?', [visit.idvisit], function(err, actions) {
                          visit = {
                            OpenId: openId,
                            TID: site.tid,
                            AddTime: +new Date(visit.visit_last_action_time) / 1000,
                            Type: 'piwik',
                            PiwikData: {
                              Name: site.name,
                              VisitorId: visit.idvisitor.toString('hex'),
                              Details: actions.map(function(action) {
                                return {
                                  Title: action.name,
                                  AddTime: +new Date(action.server_time) / 1000,
                                  SpentTime: action.time_spent_ref_action
                                };
                              })
                            }
                          };
                          visitsToSave.push(visit);
                          return callback2();
                        });
                      }
                    });
                  });
                  return callback();
                });
              });
            }
          });
        });
      };
      return async.auto({
        sites: getAllSites,
        visitorIds: getAllVisitorIds,
        details: ['sites', 'visitorIds', getVisitsDetail]
      }, function(err) {
        console.log('update piwik done', new Date - beginTime);
        return callback(err);
      });
    }
  };
};
