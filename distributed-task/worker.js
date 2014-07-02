// Distributed Task
// File: worker.js
// Desc: the worker client

'use strict';

var tls = require('tls'),
    util = require('util'),
    events = require('events'),
    os = require('os'),
    redis = require('redis'),
    netev = require('netev'),
    utils = require('./utils.js');


var Worker = function() {
    events.EventEmitter.call(this);
    this.log_prefix = '[Worker] ';
    this.redis_up = false;

    this.task_definitions = {};
    this.distributors = {};
    this.tasks = {};

    this.init = function(config) {
        var self = this;

        this.share_key = config.share_key,
        this.debug = config.debug || false;
        this.debug_netev = config.debug_netev || false;
        if(this.debug) {
            utils.log.call(self, 'debug enabled');
            utils.log.call(self, 'config dump', false, config);
        }

        // Start the monitoring loop
        setInterval(function() {
            _loop.call(self, config);
        }, config.loop_interval || 60000);
        _loop.call(this, config);
    };

    this.addTask = function(task_name, object) {
        this.task_definitions[task_name] = object;
        utils.log.call(this, 'task added', task_name);
    };

    this.stopDistributorTasks = function(distributor) {
        utils.log.call(this, 'stopping tasks for distributor', distributor.hostname);
    };

    this.getTasks = function() {
        return this.tasks;
    };

    this.stopTask = function(task_id) {
        var task = this.tasks[task_id];
        if(!task) return false;

        // Send stop event to task
        task.events.emit('stop');
        utils.log.call(this, 'requested task stop', task_id);
    };

    this.stopAllTasks = function() {
        utils.log.call(this, 'stopping all tasks...');
        for(var key in this.tasks) {
            this.stopTask(key);
        }
    };

    var _addTask = function(task, hostname) {
        var self = this;

        // Events used to send signals to the task (which normally refers to the object as 'manager')
        // + some helper functions for nice debugging
        var evs = new events.EventEmitter();
        evs.log = function() {
            var args = Array.prototype.slice.call(arguments, 0);
            args.unshift('[task: ' + task.id + ']');
            utils.log.apply(self, args);
        },
        evs.error = function() {
            var args = Array.prototype.slice.call(arguments, 0);
            args.unshift('[task: ' + task.id + ']');
            utils.error.apply(self, args);
        };

        // Create new 'process' object from task_definitions
        // tasks are eventemitters, so we subscribe to it
        var process = new this.task_definitions[task.function](evs, task);

        // Subscribe to its _events
        process.on('_stop', function() {
            utils.log.call(self, 'task stopped', hostname, task.id);
            // Set state to STOPPED in Redis (distributor cleans up)
            self.redis.hset('task-' + task.id, 'state', 'STOPPED');
        });
        process.on('_update', function() {
            self.redis.hset('task-' + task.id, 'update', new Date().getTime());
        });
        process.on('_end', function() {
            utils.log.call(self, 'task ended', task.id);
            self.redis.hset('task-' + task.id, 'state', 'END');
        });

        // Store the task
        this.tasks[task.id] = {
            hostname: hostname,
            process: process,
            events: evs
        }

        utils.log.call(this, 'task added', hostname, task.id);
    };

    this.onAddTask = function(task_id, hostname) {
        var self = this;

        // Get task data from Redis
        this.redis.hget('task-' + task_id, 'data', function(err, data) {
            if(err)
                return utils.error.call(self, 'get Redis task error, task_id: ' + task_id, err);

            _addTask.call(self, JSON.parse(data), hostname);
        });
    };

    var _onRedisUp = function(redis_client) {
        this.redis = redis_client;

        // For each distributor, send redisUp notification
        for(var key in this.distributors) {
            var dist = this.distributors[key];
            if(!dist || dist === true) continue;

            dist.emit('redisUp');
        }
        utils.log.call(this, 'connected to Redis');
    };

    var _onRedisDown = function(err) {
        this.redis = undefined;

        // For each distributor, send redisDown notification
        for(var key in this.distributors) {
            var dist = this.distributors[key];
            if(!dist || dist === true) continue;
            dist.emit('redisDown');
        }
        utils.error.call(this, 'Redis is down', err);
    };

    var _loop = function(config) {
        this.emit('loop');
        var self = this;

        // Send distributor healthchecks
        for(var key in this.distributors) {
            var dist = this.distributors[key];
            if(!dist || dist === true) continue;

            dist.emit('healthCheck', os.loadavg()[1]);
            utils.log.call(this, 'sending healthcheck', dist.hostname);
        }

        // Check Redis
        //      true = connecting
        //      false = connection error
        //      undefined = other error, requeue
        if(this.redis == false || this.redis == undefined) {
            if(this.redis == undefined)
                utils.log.call(this, 'connecting to Redis...');

            this.redis = true;
            var redis_client = redis.createClient(config.redis.port, config.redis.host, {
                enable_offline_queue: false
            });
            redis_client.on('ready', function() {
                _onRedisUp.call(self, redis_client);
            });
            redis_client.on('error', function(err) {
                if(self.redis === true) {
                    setTimeout(function() { self.redis = false; }, self.backoff || 10000);
                } else {
                    _onRedisDown.call(self, err);
                    delete self.redis;
                }
            });
        } else if(this.redis !== true) {
            try {
                this.redis.ping();
            } catch(err) {
                _onRedisDown.call(this, err);
            }
        }

        // Check disconnected distributors
        // this.distributors:
        //      true = connecting
        //      false = connection error
        //      undefined = other error, requeue

        for(var i=0; i<config.distributors.length; i++) {
            (function(i) {
                var dist_config = config.distributors[i],
                    hostname = dist_config[0] + ':' + dist_config[1],
                    distributor = self.distributors[hostname];

                if(distributor === undefined || distributor === false) {
                    if(distributor === undefined)
                        utils.log.call(self, 'connecting to distributor', hostname);

                    // Connecting state
                    self.distributors[hostname] = true;
                    var connection = tls.connect({
                        host: dist_config[0],
                        port: dist_config[1],
                        rejectUnauthorized: false
                    });

                    var stream = connection.on('secureConnect', function() {
                        // Distributor expects share key immediately
                        stream.write(self.share_key);

                        // Sent upon correct share_key, dist already netev wrapped
                        utils.receiveUntil(stream, 'HELLO WORKER', function(stream) {
                            // Wrap  the stream for incoming events
                            var dist = netev(stream, self.debug_netev);
                            dist.hostname = hostname;
                            self.distributors[hostname] = dist;

                            // Immediately emit hostname, we are ready for tasks!
                            dist.emit('hostname', os.hostname());
                            self.emit('distributorConnected', dist_config);

                            // Receive tasks
                            dist.on('addTask', function(task_id) {
                                self.onAddTask(task_id, hostname);
                            });

                            // If Redis, this will activate the worker
                            if(self.redis) {
                                dist.emit('redisUp');
                            }

                            utils.log.call(self, 'distributor added', dist.hostname);
                        }, {timeout: 10});
                    });

                    connection.on('end', function() {
                        var distributor = self.distributors[hostname];

                        // Not true or false (not connecting)
                        if(typeof distributor != 'boolean') {
                            utils.log.call(self, 'distributor disconnected', hostname);
                            self.stopDistributorTasks(distributor);
                            delete self.distributors[hostname];
                        }
                    });
                    connection.on('error', function(err) {
                        var distributor = self.distributors[hostname];

                        // Connect failure
                        if(distributor === true) {
                            setTimeout(function() { self.distributors[hostname] = false; }, self.backoff || 10000);
                        } else {
                            utils.log.call(self, 'distributor error', hostname, err);
                            self.stopDistributorTasks(distributor);
                            delete self.distributors[hostname];
                        }
                    });
                }
            })(i);
        }
    };
};

util.inherits(Worker, events.EventEmitter);
module.exports = new Worker();
