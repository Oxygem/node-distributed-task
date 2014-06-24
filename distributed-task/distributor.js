// Distributed Task
// File: distributor.js
// Desc: distributes and monitors tasks

'use strict';

var tls = require('tls'),
    fs = require('fs'),
    util = require('util'),
    events = require('events'),
    redis = require('redis'),
    netev = require('netev'),
    utils = require('./utils.js');


var Distributor = function() {
    events.EventEmitter.call(this);
    this.log_prefix = '[Distributor] ';

    this.config = {};
    this.workers = [];
    this.tasks = {}; // maps task_id => worker
    this.server = null;

    this.init = function(config) {
        var self = this;

        this.share_key = config.share_key,
        this.debug = config.debug || false,
        this.debug_netev = config.debug_netev || false;
        if(this.debug) {
            utils.log.call(self, 'debug enabled');
            utils.log.call(self, 'config dump', false, config);
        }

        // Start the server workers connect to
        var server = tls.Server({
            key: fs.readFileSync(config.key),
            cert: fs.readFileSync(config.cert)
        });
        server.listen(config.port, config.host);        // Bind up server
        server.on('secureConnection', function(stream) {
            _addWorker.call(self, stream);
            utils.log.call(self, 'client connected', 'verifying worker...');
        });
        server.on('error', function(err) {
            utils.error.call(self, err.toString());
            process.exit(1);
        });
        server.on('listening', function() {
            this.server = server;
            self.emit('ready');
            utils.log.call(self, 'listening for workers on', config.port);
        });

        // Start the monitoring loop
        setInterval(function() {
            _loop.call(self, config);
        }, config.loop_interval || 60000);
        _loop.call(this, config);
    };

    var _addWorker = function(stream) {
        var self = this;

        utils.receiveUntil(stream, this.share_key, function(stream) {
            var worker = netev(stream, self.debug_netev);
            self.workers.push(worker);

            // Notify worker, expect event
            stream.write('HELLO WORKER');
            // When the worker sends its hostname (immediate), activate
            worker.on('hostname', function(hostname) {
                worker.active = false,
                worker.hostname = hostname,
                worker.load = 0;
                self.emit('workerAdded', hostname);
                utils.log.call(self, 'worker added', hostname);
            });

            // Monitor health
            worker.on('healthCheck', function(load) {
                worker.load = load;
                utils.log.call(self, 'received health check', worker.hostname + ': ', load);
            });

            // Monitor workers Redis connection
            worker.on('redisDown', function() {
                worker.active = false;
                utils.log.call(self, 'worker Redis down', worker.hostname);
            });
            worker.on('redisUp', function() {
                worker.active = true;
                utils.log.call(self, 'worker Redis up', worker.hostname);
            });

            // Monitor exits
            // workers are expected to detect disconnects and stop tasks flagged as such
            stream.on('end', function() {
                worker.active = false;
                self.workers = self.workers.slice(self.workers.indexOf(worker));
                utils.log.call(self, 'worker disconnected', worker.hostname);
            });
            stream.on('error', function(err) {
                utils.error.call(self, 'workers stream error', err);
            });
        }, {timeout: 10});
    };

    this.getNewTasks = function() {
        var self = this;

        this.redis.rpop('new-task', function(err, reply) {
            if(!reply) return;
            if(err)
                return utils.error.call(self, err);

            var task_data;
            try {
                task_data = JSON.parse(reply);
            } catch(e) {
                return utils.error.call(self, 'invalid task JSON', reply);
            }
            if(!task_data.id || !task_data.function || !task_data.data) {
                return utils.error.call(self, 'invalid task', task_data);
            }

            self.addTask(task_data);
        });
    };

    this.addTask = function(task_data) {
        var self = this,
            low_load_worker,
            now = new Date().getTime();

        // Pick worker with lowest load
        for(var i=0; i<this.workers.length; i++) {
            var worker = this.workers[i];
            if(!worker.active) continue;

            if(!low_load_worker)
                low_load_worker = worker;
            else(worker.load < low_load_worker.load)
                low_load_worker = worker;
        }
        if(!low_load_worker) {
            this.redis.lpush('new-task', JSON.stringify(task_data));
            return utils.error.call(this, 'no active workers for task', task_data.id);
        }

        var task_key = 'task-' + task_data.id;

        // Copy task_id into Redis list & task_data atomically
        this.redis.multi()
            .sadd('tasks', task_data.id)
            .hset(task_key, 'state', 'RUNNING')
            .hset(task_key, 'start', now)
            .hset(task_key, 'update', now)
            .hset(task_key, 'data', JSON.stringify(task_data))
            // On callback send task_id to chosen worker + add to this.tasks
            .exec(function(err, replies) {
                utils.log.call(self, 'task sent', 'worker: ' + low_load_worker.hostname, 'task_id: ' + task_data.id);
                self.tasks[task_data.id] = low_load_worker;
                low_load_worker.emit('addTask', task_data.id);
            });
    };

    this.checkTasks = function(task_ids) {
        var self = this;

        // Loop tasks, check timestamp recent
        for(var i=0; i<task_ids.length; i++) {
            (function(i) {
                // Get task state and update
                var task_id = task_ids[i];
                this.redis.hmget('task-' + task_id, ['state', 'update'], function(err, reply) {
                    if(err)
                        return utils.error.call(self, 'Redis error checking task', task_id);

                    var state = reply[0],
                        update = reply[1];

                    // Requeue stopped
                    if(state == 'STOPPED') {
                        self.requeueTask(task_id);
                    // Check update within time limit
                    } else if(state == 'RUNNING') {
                        var now = new Date.getTime();
                        // Requeue if not
                        if(now - update > self.config.task_timeout) {
                            self.requeueTask(task_id);
                        }
                    // Clean up
                    } else if(state == 'END') {
                        // TODO: check if task cleanup is true, if so shift task_id to cleanup queue for external process
                        self.removeTask(task_id);
                    // Unknown state
                    } else {
                        // TODO: delete/log alien states
                    }
                });
            })(i);
        }
    };

    this.checkAllTasks = function() {
        utils.log.call(this, 'checking all tasks...');

        // Get task set from Redis, send to checkTasks
        this.redis.sget('tasks', function(err, reply) {
            console.log(reply);
        });
    };

    this.requeueTask = function(task_id) {
        var self = this;

        // Atomically remove task-id hash and push task_id to new-task list
    };

    this.removeTask = function(task_id) {
        var self = this;

        // Atomically remove task-id hash and task_id list from tasks list
    }

    this.getWorkers = function() {
        return this.workers;
    };

    this.closeWorker = function(worker) {
        utils.log.call(this, 'closing worker', worker.hostname);
        worker.stream.end();
    };

    this.closeAllWorkers = function() {
        utils.log.call(this, 'closing all workers...');
        for(var i=0; i<this.workers.length; i++) {
            this.closeWorker(this.workers[i]);
        }
        this.workers = [];
    }

    var _onRedisDown = function(err) {
        if(this.redis_up) {
            utils.error.call(this, 'Redis is down', err);
            this.redis_up = false;
            this.redis = null;
        }
    };

    // Monitor loop, finds tasks not being updated properly and re-assigns
    var _loop = function(config) {
        this.emit('loop');
        var self = this;

        // Check for Redis
        if(this.redis === undefined || this.redis === false) {
            if(this.redis === undefined)
                utils.log.call(this, 'connecting to Redis...');

            // Attempt to connect
            this.redis = true;
            var redis_client = redis.createClient(config.redis.port, config.redis.host, {
                enable_offline_queue: false
            });
            redis_client.on('ready', function() {
                self.redis = redis_client;
                utils.log.call(self, 'connected to Redis');
            });
            redis_client.on('error', function(err) {
                if(self.redis === true) {
                    setTimeout(function() { self.redis = false; }, self.backoff || 10000);
                } else {
                    utils.error.call(self, 'Redis is down', err);
                    delete self.redis;
                }
            });
        } else if(this.redis !== true) {
            try {
                this.redis.ping();

                this.getNewTasks();
                this.checkTasks(Object.keys(this.tasks));

                if(this.loops % this.check_all_interval == 0){
                    this.checkAllTasks();
                }
            } catch(err) {
                _onRedisDown.call(this, err);
            }
        }
    };
};

util.inherits(Distributor, events.EventEmitter);
module.exports = new Distributor();
