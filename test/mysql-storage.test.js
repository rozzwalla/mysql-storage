'use strict';

const HOST     = 'reekoh-staging.cg1corueo9zh.us-east-1.rds.amazonaws.com',
	  PORT     = 3306,
	  USER     = 'reekoh',
	  PASSWORD = 'rozzwalla',
	  DATABASE = 'reekoh',
	  TABLE    = 'reekoh_table';

var cp     = require('child_process'),
	assert = require('assert'),
	storage;

describe('Storage', function () {
	this.slow(8000);

	after('terminate child process', function () {
		storage.kill('SIGKILL');
	});

	describe('#spawn', function () {
		it('should spawn a child process', function () {
			assert.ok(storage = cp.fork(process.cwd()), 'Child process not spawned.');
		});
	});

	describe('#handShake', function () {
		it('should notify the parent process when ready within 8 seconds', function (done) {
			this.timeout(8000);

			storage.on('message', function (message) {
				if (message.type === 'ready')
					done();
			});

			storage.send({
				type: 'ready',
				data: {
					options: {
						host: HOST,
						port: PORT,
						user: USER,
						password: PASSWORD,
						database: DATABASE,
						table: TABLE,
						fields: JSON.stringify({
							string_type: {
								source_field: 'name', data_type: 'String'
							}
						})
					}
				}
			}, function (error) {
				assert.ifError(error);
			});
		});
	});

	describe('#data', function () {
		it('should process the data', function (done) {
			storage.send({
				type: 'data',
				data: {
					name: 'rozz'
				}
			}, done);
		});
	});
});