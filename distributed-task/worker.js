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

    };

    this.stopAllTasks = function(task_id) {

    };

    this.onAddTask = function(task_id) {
        utils.log.call(this, 'task added', task_id);

        // Get task from Redis

        // Create new object from task_definitions

        // Subscribe to its _events
    };

    var _onRedisDown = function(err) {
        if(this.redis_up) {
            this.redis_up = false;
            this.redis = null;

            // For each distributor, send redisDown notification
            for(var key in this.distributors) {
                var dist = this.distributors[key];
                if(!dist || dist === true) continue;

                dist.emit('redisDown');
            }
            utils.error.call(this, 'Redis is down', err);
        }
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
        if(!this.redis_up) {
            if(!this.redis)
                utils.log.call(this, 'connecting to Redis...');

            var redis_client = redis.createClient(config.redis.port, config.redis.host, {
                enable_offline_queue: false
            });
            redis_client.on('ready', function() {
                if(!self.redis_up) {
                    self.redis_up = true;

                    // For each distributor, send redisUp notification
                    for(var key in self.distributors) {
                        var dist = self.distributors[key];
                        if(!dist || dist === true) continue;

                        dist.emit('redisUp');
                    }
                    utils.log.call(self, 'connected to Redis');
                }
            });
            redis_client.on('error', function(err) {
                _onRedisDown.call(self, err);
            });
            this.redis = redis_client;
        } else {
            try {
                this.redis.ping();
            } catch(err) {
                _onRedisDown.call(this, err);
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
                                    self.onAddTask(task_id);
                                });

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
        }
    };
};

util.inherits(Worker, events.EventEmitter);
module.exports = new Worker();
