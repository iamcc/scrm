// Generated by CoffeeScript 1.7.1
var async;

async = require('async');

module.exports = function(db, db2, toObjectId) {
  var __type;
  db.bind('tb_module_ugc_group_data');
  __type = '社会化拼团';
  return {
    run: function(time, callback) {
      var getMembers, updateLogs, updateMembers, updateOpenIds, _time;
      _time = new Date;
      console.log('begin ugc group');
      getMembers = function(cb) {
        return db.tb_module_ugc_group_data.aggregate([
          {
            $match: {
              AddTime: {
                $gt: time
              }
            }
          }, {
            $group: {
              _id: '$GuestID',
              TIDs: {
                $addToSet: '$TID'
              },
              Contacts: {
                $addToSet: {
                  Name: '$Name',
                  Mobile: '$Mobile'
                }
              },
              LogIds: {
                $push: '$_id'
              }
            }
          }
        ], function(err, docs) {
          var members;
          members = {};
          docs.forEach(function(member) {
            return members[member._id] = {
              GuestId: member._id,
              TIDs: member.TIDs,
              Contacts: member.Contacts.filter(function(c) {
                return c.Name;
              }),
              LogIds: member.LogIds
            };
          });
          return cb(err, members);
        });
      };
      updateOpenIds = function(cb, rst) {
        var k, m, members;
        members = rst.members;
        return db.tb_module_oauth_guest.find({
          _id: {
            $in: (function() {
              var _results;
              _results = [];
              for (k in members) {
                m = members[k];
                _results.push(m.GuestId);
              }
              return _results;
            })()
          }
        }).toArray(function(err, docs) {
          rst.members = docs.map(function(member) {
            return {
              OpenId: member.WxOpenID,
              NickName: member.RawData && member.RawData.nickname || '',
              Contacts: rst.members[member._id].Contacts,
              TIDs: rst.members[member._id].TIDs,
              LogIds: rst.members[member._id].LogIds
            };
          });
          return cb(err);
        });
      };
      updateMembers = function(cb, rst) {
        return async.each(rst.members, function(member, cbEach) {
          return db2.members.update({
            OpenId: member.OpenId
          }, {
            $set: {
              OpenId: member.OpenId,
              NickName: member.NickName
            },
            $addToSet: {
              Contacts: {
                $each: member.Contacts
              },
              TIDs: {
                $each: member.TIDs
              },
              Tags: __type
            }
          }, {
            upsert: true
          }, cbEach);
        }, cb);
      };
      updateLogs = function(cb, rst) {
        return async.each(rst.members, function(member, cbEach) {
          return db.tb_module_ugc_group_data.find({
            _id: {
              $in: member.LogIds
            }
          }).toArray(function(err, logs) {
            logs = logs.map(function(log) {
              return {
                OpenId: member.OpenId,
                Mobile: log.Mobile,
                Name: log.Name,
                AddTime: log.AddTime,
                TID: log.TID,
                DataId: log.GroupID,
                Type: __type
              };
            });
            return db2.logs.insert(logs, cbEach);
          });
        }, cb);
      };
      return async.auto({
        members: getMembers,
        updateOpenIds: ['members', updateOpenIds],
        updateMembers: ['updateOpenIds', updateMembers],
        updateLogs: ['updateOpenIds', updateLogs]
      }, function(err) {
        console.log('end ugc group', new Date - _time);
        return callback(err);
      });
    }
  };
};
