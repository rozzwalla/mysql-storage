'use strict';

var _        = require('lodash'),
	async    = require('async'),
	mysql    = require('mysql'),
	moment   = require('moment'),
	isJSON   = require('is-json'),
	platform = require('./platform'),
	tableName, parseFields, connection;

/*
 * Listen for the data event.
 */
platform.on('data', function (data) {
	if (isJSON(data, true)) {
		var saveData = {};

		async.forEachOf(parseFields, function (field, key, callback) {
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
						}
						else if (field.data_type === 'Integer') {
							var intData = parseInt(datum);

							if (isNaN(intData))
								processedDatum = datum; //store original value
							else
								processedDatum = intData;
						}
						else if (field.data_type === 'Float') {
							var floatData = parseFloat(datum);

							if (isNaN(floatData))
								processedDatum = datum; //store original value
							else
								processedDatum = floatData;
						}
						else if (field.data_type === 'Boolean') {
							var type = typeof datum;

							if ((type === 'string' && datum.toLocaleLowerCase() === 'true') ||
								(type === 'number' && datum === 1 )) {
								processedDatum = true;
							}
							else if ((type === 'string' && datum.toLocaleLowerCase() === 'false') ||
								(type === 'number' && datum === 0 )) {
								processedDatum = false;
							}
							else
								processedDatum = datum;
						}
						else if (field.data_type === 'DateTime') {

							var dtm = new Date(datum);
							if (!isNaN(dtm.getTime())) {
								if (field.format !== undefined)
									processedDatum = moment(dtm).format(field.format);
								else
									processedDatum = dtm;
							}
							else
								processedDatum = datum;
						}
					}
					catch (e) {
						console.error('Data conversion error in MySQL.', e);
						platform.handleException(e);
						processedDatum = datum;
					}
				}
				else
					processedDatum = datum;
			}
			else
				processedDatum = null;

			saveData[key] = processedDatum;
			callback();
		}, function () {
			connection.query('INSERT INTO ' + tableName + ' SET ?', saveData, function (error, result) {
				if (error) {
					console.error('Failed to save record in MySQL.', error);
					platform.handleException(error);
				}
				else {
					platform.log(JSON.stringify({
						title: 'Record Successfully inserted to MySQL.',
						data: result
					}));
				}
			});
		});
	}
	else {
		console.error('Invalid Data not in JSON Format for MySQL Plugin.', data);
		platform.handleException(new Error('Invalid Data not in JSON Format for MySQL Plugin. ' + data));
	}
});

/*
 * Event to listen to in order to gracefully release all resources bound to this service.
 */
platform.on('close', function () {
	try {
		connection.end(function (error) {
			if (error) platform.handleException(error);
			platform.notifyClose();
		});
	}
	catch (error) {
		console.error(error);
	}
});

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {
	parseFields = JSON.parse(options.fields);

	async.forEachOf(parseFields, function (field, key, callback) {
		if (_.isEmpty(field.source_field))
			callback(new Error('Source field is missing for ' + key + ' in MySQL Plugin'));
		else if (field.data_type && (field.data_type !== 'String' &&
			field.data_type !== 'Integer' && field.data_type !== 'Float' &&
			field.data_type !== 'Boolean' && field.data_type !== 'DateTime')) {

			callback(new Error('Invalid Data Type for ' + key + ' allowed data types are (String, Integer, Float, Boolean, DateTime) in MySQL Plugin'));
		}
		else
			callback();
	}, function (error) {
		if (error) {
			console.error('Error parsing JSON field configuration for MySQL.', error);
			return platform.handleException(error);
		}

		tableName = options.table;

		connection = mysql.createConnection({
			host: options.host,
			port: options.port,
			user: options.user,
			password: options.password,
			database: options.database
		});

		connection.connect(function (err) {
			if (err) {
				console.error('Error connecting to MySQL.', err);
				platform.handleException(err);
			}
			else {
				platform.log('MySQL Storage Initialized.');
				platform.notifyReady();
			}
		});
	});
});
