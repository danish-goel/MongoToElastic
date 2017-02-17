'use strict';
/*global process*/
var configuration;
try {
	configuration = require('./' + (process.env.NODE_ENV || 'development') + '.json');
} catch (ex) {
	console.log(new Error('Configuration file not found for '+ (process.env.NODE_ENV || 'development')+' environment.'));
	process.exit(1);
}
module.exports = configuration;