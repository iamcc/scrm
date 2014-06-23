// Generated by CoffeeScript 1.7.1
var activity, async, boat, db, db2, fishing, invitative, mongo, plant, shake, signin, survey, toObjectId, ugcgroup, updateCount, updateLastActiveTime, updateMembersNameMobile, updateVisitorIds;

async = require('async');

mongo = require('mongoskin');

toObjectId = mongo.helper.toObjectID;

db = mongo.db("mongodb://localhost/app_mae", {
  native_parser: true
});

db2 = mongo.db("mongodb://localhost/app_mae_analysis", {
  native_parser: true
});

db.bind('tb_module_member');

db.bind('tb_module_oauth_guest');

db2.bind('members');

db2.bind('logs');

activity = require('./activity')(db, db2, toObjectId);

boat = require('./boat')(db, db2, toObjectId);

fishing = require('./fishing')(db, db2, toObjectId);

invitative = require('./invitative')(db, db2, toObjectId);

plant = require('./plant')(db, db2, toObjectId);

shake = require('./shake')(db, db2, toObjectId);

signin = require('./signin')(db, db2, toObjectId);

survey = require('./survey')(db, db2, toObjectId);

ugcgroup = require('./ugcgroup')(db, db2, toObjectId);

updateMembersNameMobile = function() {
  console.log('begin update members name and mobile');
  return db2.members.find({
    Name: {
      $exists: false
    }
  }).toArray(function(err, members) {
    return async.each(members, function(member, callback) {
      var contact;
      if (member.Contacts.length) {
        contact = member.Contacts.slice(-1)[0];
        return db2.members.update({
          _id: member._id
        }, {
          $set: {
            Name: contact.Name,
            Mobile: contact.Mobile
          }
        }, callback);
      } else {
        return callback();
      }
    }, function(err) {
      return console.log('update members name and mobile done', err);
    });
  });
};

updateVisitorIds = function(done) {
  console.log('begin update visitor ids');
  return async.auto({
    openIds: function(cb) {
      return db2.members.find({}, {
        fields: {
          _id: 0,
          OpenId: 1
        }
      }).toArray(function(err, docs) {
        return cb(err, docs.map(function(d) {
          return d.OpenId;
        }));
      });
    },
    member: [
      'openIds', function(cb, rst) {
        return db.tb_module_member.aggregate([
          {
            $unwind: '$OAuth'
          }, {
            $unwind: '$PiwikVisitorID'
          }, {
            $match: {
              PiwikVisitorID: {
                $exists: true
              },
              'OAuth.OpenID': {
                $in: rst.openIds
              }
            }
          }, {
            $group: {
              _id: '$OAuth.OpenID',
              PiwikVisitorIDs: {
                $addToSet: '$PiwikVisitorID'
              }
            }
          }
        ], function(err, docs) {
          return async.each(docs, function(doc, cbEach) {
            return db2.members.update({
              OpenId: doc._id
            }, {
              $addToSet: {
                PiwikVisitorIDs: {
                  $each: doc.PiwikVisitorIDs
                }
              }
            }, {
              upsert: true
            }, cbEach);
          }, cb);
        });
      }
    ],
    guest: [
      'openIds', function(cb, rst) {
        return db.tb_module_oauth_guest.find({
          _id: {
            $in: rst.openIds
          }
        }).toArray(function(err, docs) {
          return async.each(docs, function(doc, cbEach) {
            return db2.members.update({
              OpenId: doc._id
            }, {
              $addToSet: {
                PiwikVisitorIDs: {
                  $each: doc.PiwikVisitorID
                }
              }
            }, {
              upsert: true
            }, cbEach);
          }, cb);
        });
      }
    ]
  }, done);
};

updateCount = function(time) {
  console.log('begin update count');
  return db2.logs.aggregate([
    {
      $match: {
        $and: [
          {
            AddTime: {
              $gt: time
            }
          }, {
            Type: {
              $ne: 'piwik'
            }
          }
        ]
      }
    }, {
      $group: {
        _id: '$OpenId',
        offline: {
          $sum: {
            $cond: [
              {
                $or: [
                  {
                    $eq: ['$Type', '摇一摇']
                  }, {
                    $eq: ['$Type', '星光大道']
                  }
                ]
              }, 1, 0
            ]
          }
        },
        online: {
          $sum: {
            $cond: [
              {
                $and: [
                  {
                    $ne: ['$Type', '摇一摇']
                  }, {
                    $ne: ['$Type', '星光大道']
                  }
                ]
              }, 1, 0
            ]
          }
        }
      }
    }, {
      $match: {
        $or: [
          {
            offline: {
              $gt: 0
            }
          }, {
            online: {
              $gt: 0
            }
          }
        ]
      }
    }
  ], function(err, docs) {
    if (err) {
      return console.log(err);
    }
    docs.forEach(function(doc) {
      return db2.members.update({
        OpenId: doc._id
      }, {
        $inc: {
          offline: doc.offline,
          online: doc.online
        }
      }, function() {});
    });
    return console.log('update count done');
  });
};

updateLastActiveTime = function(time) {
  console.log('begin update last active time');
  return db2.logs.aggregate([
    {
      $match: {
        $and: [
          {
            AddTime: {
              $gt: time
            }
          }, {
            Type: {
              $ne: 'piwik'
            }
          }
        ]
      }
    }, {
      $sort: {
        AddTime: 1
      }
    }, {
      $group: {
        _id: '$OpenId',
        lastActiveTime: {
          $last: '$AddTime'
        }
      }
    }
  ], function(err, docs) {
    if (err) {
      return console.error(err);
    }
    return async.each(docs, function(doc, cbEach) {
      return db2.members.update({
        OpenId: doc._id
      }, {
        $set: {
          LastActiveTime: doc.lastActiveTime
        }
      }, cbEach);
    }, function(err) {
      return console.log('update last active time done', err);
    });
  });
};

db2.logs.findOne({}, {
  sort: {
    AddTime: -1
  },
  limit: 1
}, function(err, log) {
  var beginTime, time;
  if (err) {
    return console.error(err);
  }
  beginTime = new Date;
  time = log && log.AddTime || 0;
  console.log('lasttime', time);
  return async.auto([
    function(cb) {
      return activity.run(time, cb);
    }, function(cb) {
      return boat.run(time, cb);
    }, function(cb) {
      return fishing.run(time, cb);
    }, function(cb) {
      return invitative.run(time, cb);
    }, function(cb) {
      return plant.run(time, cb);
    }, function(cb) {
      return shake.run(time, cb);
    }, function(cb) {
      return signin.run(time, cb);
    }, function(cb) {
      return survey.run(time, cb);
    }, function(cb) {
      return ugcgroup.run(time, cb);
    }
  ], function(err) {
    updateMembersNameMobile();
    updateCount(time);
    updateLastActiveTime(time);
    return updateVisitorIds(function(err, rst) {
      console.log('update visitor ids done', new Date - beginTime, err);
      return require('./piwik')(db2).run(time, function(err) {
        console.log('all done', new Date - beginTime, err);
        return process.exit();
      });
    });
  });
});