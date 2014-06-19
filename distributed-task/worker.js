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
    this.log_prefix = '[Worker]: ';
    this.redis_up = false;

    this.task_definitions = {};
    this.distributors = {};
    this.tasks = {};

    this.stopDistributorTasks = function(distributor) {
        utils.log.call(this, 'stopping tasks for distributor: ' + distributor.hostname);
    };

    this.init = function(config) {
        var self = this;

        this.share_key = config.share_key,
        this.debug = config.debug || false;
        this.debug_netev = config.debug_netev || false;
        if(this.debug) {
            utils.log.call(self, 'debug enabled');
            utils.log.call(self, 'config dump: ', config);
        }

        // Start the monitoring loop
        setInterval(function() {
            _loop.call(self, config);
        }, config.loop_interval || 60000);
        _loop.call(this, config);
    };

    this.addTask = function(task_name, object) {
        this.task_definitions[task_name] = object;
    };

    this.onAddTask = function(task_id) {
        utils.log.call(this, 'task added: ' + task_id);

        // Get task from Redis

        // Create new object from task_definitions

        // Subscribe to its events
    };

    var _onRedisDown = function(err) {
        if(this.redis_up) {
            utils.error.call(this, 'Redis is down', err);
            this.redis_up = false;
            this.redis = null;
        }
    };

    var _loop = function(config) {
        this.emit('loop');
        var self = this;

        // Send distributor healthchecks
        for(var key in this.distributors) {
            var dist = this.distributors[key];
            if(!dist) continue;

            dist.emit('healthCheck', os.loadavg()[1]);
            utils.log.call(this, 'sending healthcheck to: ' + dist.hostname);
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
                    utils.log.call(self, 'connected to Redis');
                    self.redis_up = true;
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
            for(var i=0; i<config.distributors.length; i++) {
                var distributor = config.distributors[i],
                    hostname = distributor[0] + ':' + distributor[1];

                if(!this.distributors[hostname]) {
                    if(this.distributors[hostname] === undefined) {
                        this.distributors[hostname] = false;
                        utils.log.call(self, 'connecting to distributor: ' + hostname);
                    }

                    var connection = tls.connect({
                        host: distributor[0],
                        port: distributor[1],
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
                            self.emit('distributorConnected', distributor);

                            // Receive tasks
                            dist.on('addTask', function(task_id) {
                                self.onAddTask(task_id);
                            });

                            utils.log.call(self, 'distributor added: ' + dist.hostname);
                        }, {timeout: 10});
                    });

                    connection.on('end', function() {
                        utils.log.call(self, 'distributor disconnected: ' + hostname);

                        self.stopDistributorTasks(self.distributors[hostname]);
                        delete self.distributors[hostname];
                    });
                    connection.on('error', function(err) {
                        if(self.distributors[hostname] === undefined) {
                            utils.error.call(self, 'worker down', err);
                        }
                    });
                }
            }
        }
    };
};

util.inherits(Worker, events.EventEmitter);
module.exports = new Worker();
