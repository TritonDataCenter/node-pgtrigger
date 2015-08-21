/*
 * driver.js: driver program for updateTrigger
 */

var mod_bunyan = require('bunyan');
var mod_cmdutil = require('cmdutil');
var mod_fs = require('fs');
var mod_moray = require('moray');
var mod_url = require('url');
var VError = require('verror');
var mod_update_trigger = require('./update-trigger');

function main()
{
	var filename, version, table, trigger, code;
	var log, client;
	var url, host, port;

	/*
	 * This program is just a driver for triggerUpdateAttempt().  main()
	 * just reads arguments, constructs a bunyan logger and Moray client,
	 * and then call that function.
	 */
	mod_cmdutil.configure({
	    'synopses': [ 'FILENAME TABLE TRIGGERNAME VERSION'],
	    'usageMessage': 'Update a versioned PostgreSQL trigger.'
	});

	if (process.argv.length != 6) {
		mod_cmdutil.usage('bad arguments');
	}

	filename = process.argv[2];
	try {
		code = mod_fs.readFileSync(filename, 'utf8');
	} catch (ex) {
		mod_cmdutil.fail(ex);
	}

	table = process.argv[3];
	trigger = process.argv[4];
	version = parseInt(process.argv[5], 10);
	if (isNaN(version)) {
		mod_cmdutil.fail('not a number: %s', process.argv[5]);
	}

	log = new mod_bunyan({
	    'name': 'update-trigger',
	    'level': process.env['LOG_LEVEL'] || 'trace'
	});

	if (!process.env['MORAY_URL']) {
		mod_cmdutil.fail(
		    'MORAY_URL must be specified in the environment.');
	}

	url = mod_url.parse(process.env['MORAY_URL']);
	host = url['hostname'];
	port = parseInt(url['port'], 10);
	if (isNaN(port)) {
		mod_cmdutil.fail('MORAY_URL port is not a number');
	}

	client = mod_moray.createClient({
	    'log': log,
	    'host': host,
	    'port': port,
	    'connectTimeout': 3000,
	    'retry': null,
	    'reconnect': false
	});

	client.on('error', function (err) {
		err = new VError(err, 'moray client error');
		log.error(err);
		mod_cmdutil.fail(err);
	});

	client.on('connect', function () {
		mod_update_trigger.triggerUpdateAttempt({
		    'log': log,
		    'moray': client,
		    'code': code,
		    'table': table,
		    'trigger': trigger,
		    'version': version
		}, function (err) {
			if (err) {
				mod_cmdutil.fail(err);
			}

			/* process exit */
			client.close();
		});
	});
}

main();
