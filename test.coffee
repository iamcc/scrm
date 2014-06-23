mongo      = require 'mongoskin'
db2        = mongo.db "mongodb://localhost/app_mae_analysis", native_parser: true

db2.bind 'members'
db2.bind 'logs'

piwik = require './piwik'
piwik = piwik db2

piwik.run 0, (err) ->
	console.log err