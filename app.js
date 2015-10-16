'use strict';

var platform    = require('./platform'),
	mysql	    = require('mysql'),
	moment	    = require('moment'),
	_			= require('lodash'),
	isJSON      = require('is-json'),
	tableName, parseFields, connection;

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {

	//try catch to capture parsing error in JSON.parse

	try {
		parseFields = JSON.parse(options.fields);

		_.forEach(parseFields, function(field, key) {
			if (field.source_field === undefined || field.source_field === null) {
				throw( new Error('Source field is missing for ' + key + ' in MySQL Plugin'));
			} else if (field.data_type  && (field.data_type !== 'String' && field.data_type !== 'Integer' &&
				field.data_type !== 'Float'  && field.data_type !== 'Boolean' &&
				field.data_type !== 'DateTime')) {
				throw(new Error('Invalid Data Type for ' + key + ' allowed data types are (String, Integer, Float, Boolean, DateTime) in MySQL Plugin'));
			}
		});

	} catch (e) {
		console.error('Error parsing JSON field configuration for MySQL.', e);
		platform.handleException(e);
		return;
	}

	tableName   = options.table;

	connection = mysql.createConnection({
		host     : options.host,
		port     : options.port,
		user     : options.user,
		password : options.password,
		database : options.database
	});

	connection.connect(function(err) {
		if (err) {
			console.error('Error connecting to MySQL.', err);
			platform.handleException(err);
		} else {
			platform.log('Connected to MySQL.');
			platform.notifyReady(); // Need to notify parent process that initialization of this plugin is done.
		}
	});

});

/*
 * Listen for the data event.
 */
platform.on('data', function (data) {

	if (isJSON(data, true)) {
		var saveData = {};

		_.forEach(parseFields, function(field, key) {

			var datum = data[field.source_field],
				processedDatum;

			if (datum !== undefined && datum !== null) {
				if (field.data_type) {
					try {
						if (field.data_type === 'String') {

							if (isJSON(datum))
								processedDatum = JSON.stringify(datum);
							else
								processedDatum = String(datum);

						} else if (field.data_type === 'Integer')  {

							var intData = parseInt(datum);

							if (isNaN(intData))
								processedDatum = datum; //store original value
							else
								processedDatum = intData;

						} else if (field.data_type === 'Float')  {

							var floatData = parseFloat(datum);

							if (isNaN(floatData))
								processedDatum = datum; //store original value
							else
								processedDatum = floatData;

						} else if (field.data_type === 'Boolean') {

							var type = typeof datum;

							if ((type === 'string' && datum.toLocaleLowerCase() === 'true') ||
								(type === 'number' && datum === 1 )) {
								processedDatum = true;
							} else if ((type === 'string' && datum.toLocaleLowerCase() === 'false') ||
								(type === 'number' && datum === 0 )) {
								processedDatum = false;
							} else {
								processedDatum = datum;
							}
						} else if (field.data_type === 'DateTime') {

							var dtm = new Date(datum);
							if (!isNaN( dtm.getTime())) {

								if (field.format !== undefined)
									processedDatum = moment(dtm).format(field.format);
								else
									processedDatum = dtm;


							} else {
								processedDatum = datum;
							}
						}
					} catch (e) {
						console.error('Data conversion error in MySQL.', e);
						platform.handleException(e);
						processedDatum = datum;
					}

				} else {
					processedDatum = datum;
				}

			} else {
				processedDatum = null;
			}

			saveData[key] = processedDatum;

		});

		connection.query('INSERT INTO ' + tableName + ' SET ?', saveData, function(error, result) {
			// Neat!
			if (error) {
				console.error('Failed to save record in MySQL.', error);
				platform.handleException(error);
			}
			else
				platform.log('Record Successfully saved to MySQL.', result.toString());
		});

	} else {

		console.error('Invalid Data not in JSON Format for MySQL Plugin.', data);
		platform.log('Invalid Data not in JSON Format for MySQL Plugin.', data);
	}


});