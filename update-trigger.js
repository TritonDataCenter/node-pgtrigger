/*
 * update-trigger.js: JavaScript function to update a versioned trigger
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_util = require('util');
var mod_vasync = require('vasync');
var VError = require('verror');
var sprintf = require('extsprintf').sprintf;

/* Public interface */
exports.triggerUpdateAttempt = triggerUpdateAttempt;
/* Testing interface */
exports.TriggerLister = TriggerLister;
exports.sqlValidName = sqlValidName;
exports.sqlListTriggers = sqlListTriggers;
exports.sqlUpdateTriggers = sqlUpdateTriggers;

/*
 * Manage versions for a named PostgreSQL table trigger.
 *
 * This function provides a mechanism for maintaining versioned triggers on a
 * PostgreSQL table using the Moray client interface.  Named arguments include:
 *
 *     log		a bunyan logger
 *
 *     moray		a Moray client
 *
 *     table		name of the PostgreSQL table
 *
 *     trigger		logical name of the trigger.  The actual PostgreSQL
 *     			trigger name will comprise this name, plus a version
 *     			suffix.  The user-defined database function will be
 *     			defined with a corresponding name.
 *
 *     code		body of the trigger function at the current version.
 *     			This should be a pl/pgSQL block not including the
 *     			"CREATE FUNCTION" preamble.
 *
 *     version		current version of the function.  Versions are just
 *     			integers, where larger integers denote newer versions.
 *
 *     dryrun		if true, then takes no action.  If action is required,
 *     (optional)	an error will be produced.
 *
 * This function assumes that the trigger should fire AFTER both "INSERT" and
 * "DELETE" operations on the table.  It could be generalized to support other
 * kinds of triggers.
 *
 * The way you use this is as follows: each version of the software that
 * maintains the trigger invokes this function with the same "table" and
 * "trigger" arguments.  If the "code" argument changes, then the "version"
 * argument must also change.  The idea is that the software that maintains the
 * trigger includes a definition of the trigger function and a version number,
 * and it passes those to this function to set up the trigger.
 *
 * This function examines the state of the database and does the following:
 *
 *    o If the trigger has never been installed, it will be installed.
 *
 *    o If a previous version of the same trigger has been installed on the same
 *      table, then a single transaction will be issued to drop the old trigger
 *      and create a new one using the new procedure.
 *
 *    o If the same or a newer version of the same trigger has been installed on
 *      the same table, then no changes will be made.
 *
 * Importantly, this function avoids issuing CREATE TRIGGER or DROP TRIGGER
 * unless necessary, since those operations take an exclusive table lock.
 *
 * This function is idempotent.  On success, the trigger is at least as up to
 * date as the requested version.  On failure, it's highly likely that no
 * changes were made to the trigger version, but it's possible (in the event of
 * a network issue or certain server-side crashes) that the change succeeded.
 * In both cases, no transactions will complete without either the old or new
 * trigger in effect.
 *
 * This function can safely be invoked by multiple versions of the same
 * software, even concurrently.  The end result will be that the latest
 * requested version of the trigger is installed, and at no point will the
 * version that is installed regress to an earlier version.
 *
 * For historical reasons, this function treats a PostgreSQL trigger whose name
 * exactly matches "trigger" (with no suffix) as equivalent to version 0 of the
 * trigger.
 */
function triggerUpdateAttempt(args, callback)
{
	var log, client, code, table, trigger, version, token, dryrun;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.moray, 'args.moray');
	mod_assertplus.string(args.table, 'args.table');
	mod_assertplus.string(args.trigger, 'args.trigger');
	mod_assertplus.string(args.code, 'args.code');
	mod_assertplus.number(args.version, 'args.version');
	mod_assertplus.equal(Math.round(args.version), args.version,
	    'version is not an integer');
	mod_assertplus.ok(args.version >= 0, 'version out of range');
	/*
	 * There's nothing wrong with versions over 100, but we're very unlikely
	 * to reach that point, and we want to blow up on wildly invalid values.
	 */
	mod_assertplus.ok(args.version < 100, 'version out of range');

	log = args.log;
	client = args.moray;
	code = args.code;
	table = args.table;
	trigger = args.trigger;
	version = args.version;
	dryrun = args.dryrun === true;

	/*
	 * Validate that the table name and trigger name conform to our strict
	 * identifier guidelines.  Once we do this, we can safely construct SQL
	 * strings that contain these strings.
	 */
	if (!sqlValidName(table)) {
		setImmediate(callback, new VError(
		    'table name contains invalid characters: "%s"', table));
		return;
	}

	if (!sqlValidName(trigger)) {
		setImmediate(callback, new VError(
		    'trigger name contains invalid characters: "%s"', trigger));
		return;
	}

	/*
	 * We use PostgreSQL dollar-quoting to avoid having to escape anything
	 * inside the function body.  It's not allowed for the code to contain
	 * the dollar-quoting delimiter.  This is extremely unlikely, but we
	 * validate that here.
	 */
	token = '$FunctionCode$';
	if (code.indexOf(token) != -1) {
		setImmediate(callback, new VError(
		    'code cannot contain token "%s"', token));
		return;
	}

	mod_vasync.waterfall([
	    function listTriggers(wfcallback) {
		var sqlstr, req, triggers;

		sqlstr = sqlListTriggers({
		    'table': table,
		    'trigger': trigger
		});

		req = client.sql(sqlstr);

		triggers = new TriggerLister({
		    'log': log,
		    'req': req,
		    'table': table,
		    'trigger': trigger,
		    'version': version
		});

		triggers.on('error', wfcallback);

		triggers.on('end', function () {
			wfcallback(null, triggers);
		});
	    },

	    function applyUpdate(triggers, wfcallback) {
		var sqlstr, req;

		sqlstr = sqlUpdateTriggers({
		    'table': table,
		    'trigger': trigger,
		    'version': version,
		    'code': code,
		    'token': token,
		    'triggers': triggers
		});

		if (sqlstr === null) {
			log.info({
			    'table': table,
			    'trigger': trigger,
			    'version': version
			}, 'triggers already up-to-date');
			wfcallback();
			return;
		}

		if (dryrun) {
			log.info(
			    'triggers not up-to-date, but dry-run requested');
			wfcallback(new Error('triggers not up-to-date, but ' +
			    'dry-run requested'));
			return;
		}

		log.info({
		    'table': table,
		    'trigger': trigger,
		    'version': version
		}, 'installing updated triggers');
		req = client.sql(sqlstr);

		req.on('error', function (err) {
			wfcallback(new VError(err, 'updating triggers'));
		});

		req.on('end', function () { wfcallback(); });
	    }
	], function (err) {
		if (err) {
			/* XXX may need to retry? */
			log.error(err, 'error updating triggers');
			callback(err);
		} else {
			log.info({
			    'table': table,
			    'trigger': trigger,
			    'version': version
			}, 'triggers up to date');
			callback();
		}
	});
}

/*
 * TriggerLister is a helper class to summarize the set of triggers found in
 * the database.  Logically, you make a Moray "sql" request to list the
 * triggers, then pass the request to this object.  This object will keep track
 * of triggers found, identify any serious issues (and emit 'error' for those),
 * and note that latest version of the trigger found (if any).  Errors from the
 * request are emitted as well.
 *
 * Named arguments in "args" include:
 *
 *     req				Moray request that returns selected rows
 *     					from PostgreSQL's trigger information
 *     					schema.
 *
 *     log, table, trigger, version	Same as the same-named arguments to
 *     					triggerUpdateAttempt().
 */
function TriggerLister(args)
{
	var self = this;

	mod_events.EventEmitter.call(this);

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.req, 'args.req');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.string(args.table, 'args.table');
	mod_assertplus.string(args.trigger, 'args.trigger');
	mod_assertplus.number(args.version, 'args.version');

	/* Save arguments. */
	self.tl_log = args.log;
	self.tl_req = args.req;
	self.tl_table = args.table;
	self.tl_trigger = args.trigger;
	self.tl_version = args.version;

	self.tl_error = null;		/* "error" emitted by Moray request */
	self.tl_ended = false;		/* "end" emitted by Moray request */

	/*
	 * Latest version number for matching triggers we've seen.  Version -1
	 * means we've seen no triggers at all.  Version 0 means we've seen the
	 * legacy (unversioned) trigger.
	 */
	self.tl_latest = -1;

	/*
	 * Count of ignored rows where we don't have reason to believe those
	 * ignored rows are meaningful.  Ignored rows would always be unusual
	 * here, since we query only for the items that we manage, but it's not
	 * necessarily a problem (as if the query is inadvertently changed to
	 * return a row that's not relevant to us).  If we find a row that
	 * really confuses us (e.g., it matches our pattern, but the version
	 * number is not an integer), we'll treat this as a more serious error
	 * below.
	 */
	self.tl_nignored = 0;

	/*
	 * Semantic error: cases where there was no issue with the Moray query
	 * per se (i.e., we got a bunch of syntactically valid rows), but where
	 * we're sufficiently confused by what we found that we cannot with
	 * confidence determine the current state of things.  Such cases would
	 * include when we find a DELETE trigger but no INSERT trigger.  That
	 * should be impossible, and means an operator has made some change, and
	 * we don't want to mess with it.
	 */
	self.tl_data_error = null;

	/*
	 * Triggers seen.  For each distinct trigger_name, we keep track of
	 * whether we've seen both the "DELETE" and "INSERT" triggers.
	 */
	self.tl_seen = {};

	/*
	 * Triggers to remove, by name.
	 */
	self.tl_toremove = {};

	/*
	 * Add handlers on the request to drive this whole mechanism.
	 */
	self.tl_req.on('record', function (row) { self.onRow(row); });

	self.tl_req.on('error', function (err) {
		mod_assertplus.ok(!self.tl_ended,
		    'moray request: "error" emitted after "end"');
		mod_assertplus.ok(self.tl_error === null,
		    'moray request: "error" emitted more than once');

		self.tl_error = err;
		self.emit('error', new VError(err, 'moray request error'));
	});

	self.tl_req.on('end', function () {
		mod_assertplus.ok(self.tl_error === null,
		    'moray request: "end" emitted after "error"');
		mod_assertplus.ok(!self.tl_ended,
		    'moray request: "end" emitted more than once');

		self.tl_ended = true;
		self.fini();
	});
}

mod_util.inherits(TriggerLister, mod_events.EventEmitter);

/*
 * [private] Invoked for each row matching the SQL query.
 */
TriggerLister.prototype.onRow = function (row)
{
	var log, name;
	var vpart, i, v;

	log = this.tl_log;

	/*
	 * Sanity-check input.  Cases where we would ignore rows are pretty
	 * unexpected.
	 */
	if (row.action_timing != 'AFTER' ||
	    (row.event_manipulation != 'DELETE' &&
	    row.event_manipulation != 'INSERT')) {
		this.tl_nignored++;
		log.warn('ignoring unexpected trigger (bad action_timing or ' +
		    'event_manipulation)', row);
		return;
	}

	name = row.trigger_name;
	if (!sqlValidName(name)) {
		this.tl_nignored++;
		log.warn('ignoring unexpected trigger (invalid name)');
		return;
	}

	/*
	 * Keep track of all triggers that we've seen so that we can later
	 * validate that we've seen the "INSERT" and "DELETE" records for each
	 * trigger.
	 */
	if (!this.tl_seen.hasOwnProperty(name)) {
		this.tl_seen[name] = [];
	}

	this.tl_seen[name].push(row);

	/*
	 * If we've already decided to remove this named trigger (which, again,
	 * should have multiple records in this query), we're done.  This
	 * doesn't count as ignoring an unexpected row -- we're just processing
	 * it by doing nothing.
	 */
	if (this.tl_toremove.hasOwnProperty(name)) {
		return;
	}

	/*
	 * Parse the version number out of the trigger's name.  Note that for
	 * compatibility, we assume that the raw trigger name is version 0.
	 */
	if (name == this.tl_trigger) {
		v = 0;
	} else {
		i = name.lastIndexOf('_v');
		if (i == -1) {
			this.tl_nignored++;
			log.warn('ignoring unexpected trigger ' +
			    '(name has no version)', row);
			return;
		}

		vpart = name.substr(i + 2);
		v = parseInt(vpart, 10);
		if (isNaN(v) || v.toString() != vpart) {
			this.tl_nignored++;
			log.warn({
			    'vpart': vpart,
			    'v': v,
			    'row': row
			}, 'ignoring unexpected trigger ' +
			'(version not a valid integer)');
			return;
		}
	}

	this.tl_latest = Math.max(v, this.tl_latest);

	if (v < this.tl_version) {
		log.debug('found previous version', row);
		this.tl_toremove[name] = 1;
		return;
	}
};

/*
 * [private] Invoked after the last row has been processed.  Runs final
 * validation checks and emits either an "error" or "end".
 */
TriggerLister.prototype.fini = function ()
{
	var name, rows, err;

	/*
	 * The final check is just to make sure that if we saw trigger records
	 * for other versions, then we saw them for both DELETE and INSERT.
	 */
	err = null;
	for (name in this.tl_seen) {
		rows = this.tl_seen[name];
		if (rows.length > 2) {
			/*
			 * This _might_ be okay, and might be handleable, but
			 * should never happen, so we treat it as a fatal error.
			 */
			err = new VError('trigger "%s": extra records', name);
			break;
		}

		if (rows.length < 2) {
			err = new VError('trigger "%s": missing records', name);
			break;
		}

		/*
		 * We already validated that "event_manipulation" is either
		 * "INSERT" or "DELETE".  As long as the two rows' values are
		 * different, then we must have one of each.
		 */
		if (rows[0].event_manipulation == rows[1].event_manipulation) {
			err = new VError(
			    'trigger "%s": missing INSERT or trigger', name);
			break;
		}
	}

	if (err !== null) {
		this.tl_data_error = err;
		this.emit('error', err);
	} else {
		this.emit('end');
	}
};

/*
 * Returns the names of triggers found for versions older than "version".
 */
TriggerLister.prototype.oldTriggers = function ()
{
	return (Object.keys(this.tl_toremove));
};

/*
 * Returns the latest version of any trigger found.  If -1, then no triggers
 * were found.  If 0, then only the legacy (unversioned) trigger was found.
 */
TriggerLister.prototype.latestVersionFound = function ()
{
	return (this.tl_latest);
};


/*
 * SQL helper functions
 */


/*
 * Returns true if the given string is valid to be used as a SQL identifier.
 * For simplicity, we use a conservative rule here, and the main goal is to
 * avoid injecting strings into SQL strings that will cause arbitrary
 * brokenness.
 */
function sqlValidName(name)
{
	return (/^[a-zA-Z0-9_]+$/.test(name));
}

/*
 * Return a SQL string to list versioned triggers on table "table" having
 * trigger name "trigger".
 */
function sqlListTriggers(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.table, 'args.table');
	mod_assertplus.string(args.trigger, 'args.trigger');

	/*
	 * While these values come from trusted sources, we want to make sure we
	 * don't accidentally do something stupid.
	 */
	mod_assertplus.ok(sqlValidName(args.table));
	mod_assertplus.ok(sqlValidName(args.trigger));

	/*
	 * This query returns any trigger on the specified table whose name
	 * exactly matches the given trigger name (which is the legacy,
	 * unversioned case) or whose name looks like the given trigger name
	 * with a version suffix.
	 */
	return (sprintf([
	    'SELECT * FROM information_schema.triggers ',
	    '    WHERE event_object_table = \'%s\' AND',
	    '    (trigger_name = \'%s\' OR ',
	    '    trigger_name ~ (\'%s\' || \'_v\\d+$\'));'
	].join(''), args.table, args.trigger, args.trigger));
}

/*
 * Returns a SQL string to update trigger definitions.  Named parameters:
 *
 *      table, trigger	See triggerUpdateAttempt().
 *      version, code
 *
 *      triggers	Set of triggers found, encapsulated in a TriggerLister
 *      		object
 *
 *	token		Dollar-quoting token to use for embedding "code" into a
 *			SQL string.
 */
function sqlUpdateTriggers(args)
{
	var stmts, newname;

	mod_assertplus.object(args, 'args');
	mod_assertplus.string(args.table, 'args.table');
	mod_assertplus.string(args.trigger, 'args.trigger');
	mod_assertplus.object(args.triggers, 'args.triggers');
	mod_assertplus.number(args.version, 'args.version');
	mod_assertplus.string(args.code, 'args.code');
	mod_assertplus.string(args.token, 'args.token');

	if (args.triggers.latestVersionFound() >= args.version) {
		return (null);
	}

	/*
	 * We want to update the triggers in a single transaction to avoid
	 * having any other transactions complete using neither the old nor the
	 * new trigger.
	 */
	stmts = [ 'BEGIN;' ];

	/*
	 * Remove old triggers.  We use "IF EXISTS" because it's okay if one of
	 * these triggers is gone by the time we go to remove it.
	 */
	args.triggers.oldTriggers().forEach(function (name) {
		mod_assertplus.ok(sqlValidName(name));
		stmts.push(sprintf(
		    'DROP TRIGGER IF EXISTS %s ON %s;', name, args.table));
	});

	/*
	 * Define the new function that we'll use for our trigger.  The function
	 * name will match the trigger name.  We don't use "OR REPLACE" because
	 * we want this transaction to fail if someone else has come in and
	 * defined this function.  We'll see that and take another lap.
	 */
	newname = sprintf('%s_v%d', args.trigger, args.version);
	mod_assertplus.ok(sqlValidName(newname));
	stmts.push(sprintf([
	    'CREATE FUNCTION %s() RETURNS TRIGGER AS ',
	    '    %s',
	    '    %s',
	    '    %s LANGUAGE plpgsql;'
	].join('\n'), newname, args.token, args.code, args.token));

	/*
	 * Define the trigger itself.
	 */
	stmts.push(sprintf('CREATE TRIGGER %s AFTER INSERT OR DELETE ON %s ' +
	    'FOR EACH ROW EXECUTE PROCEDURE %s();', newname,
	    args.table, newname));

	/*
	 * Commit the transaction.
	 */
	stmts.push(sprintf('COMMIT;'));

	return (stmts.join('\n') + '\n');
}
