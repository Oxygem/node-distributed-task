# Node Distributed Task

`distributed-task` provides a framework upon which one can easily build distributed task systems in Node. The original reason for building a new distributed task system was to allow 'watching' of tasks, specifically long/indefinitely running tasks. `distributed-task` consistes of a few features:

+ Redis as a global state store
+ `distributor`
    * handle the distribution of tasks
    * read incoming tasks from a Redis queue
    * monitors its own set of tasks
    * monitor all tasks on longer interval
+ `worker`
    * has tasks as Node functions w/JSON config
    * does the actual task work
    * connects to the distributors
    * updates task info in Redis
    * listens for watcher connections
+ `watcher`
    * talks direct to workers and can subscribe/unsubscribe to task live related events
    * gets task/worker info from Redis


## Synopsis

Shared `config.json`:

```
    {
        "debug": true,
        "debug_netev": false,
        "share_key": "abc",
        "host": "localhost",
        "port": 5000,
        "key": "test/tls/key.pem",
        "cert": "test/tls/certificate.pem",
        "loop_interval": 10000,
        "redis": {
            "host": "localhost",
            "port": 6379
        },

        "distributors": [
            ["localhost", 5000]
        ]
    }
```

`distributor.js`:

```
    #!/usr/bin/env node

    var distributor = require('../distributed-task/distributor');
    distributor.init(require('./config.json'));
```

`worker.js`:

```
    #!/usr/bin/env node

    var worker = require('../distributed-task/worker');
    worker.init(require('./config.json'));
```

## Definitions

#### Task

    {
        state_optional: true; // allow the task to continue running when Redis goes down
                              // shouldn't happen but no guarantee the task won't also be requeued w/ multiple distributors
    }

#### Task Status

+ `RUNNING` - task is running, should have updated timestamp
+ `STOPPED` - task stopped intentionally by distributor/worker loosing related worker/distributor connection
+ `END` - task is complete, pending cleanup

#### Failover/HA Policies

+ Distributors/workers send health checks to each other according to a configured time interval
+ Both distributor and worker acknowledge disconnect on stream close + after missed health check
    * distributor stops sending tasks to this worker (watching for failed tasks is separate)
    * worker stops running tasks for this distributor (which should requeue them)
+ Upon disconnect from Redis workers, for each connected distributor:
    * check distributor connected
    * if so, assumes distributor also lost Redis connection & won't requeue
    * continue distributor tasks w/o `state_optional`
    * stop all other tasks from this distributor
+ Distributors watch for tasks w/ state `RUNNING` but out of date timestamps, requeues
+ Same as above for `STOPPED`  & `END` tasks
+ Distributors watch their own tasks on a short interval, and all tasks on a long interval
