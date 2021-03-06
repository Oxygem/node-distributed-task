# Node Distributed Task

**Discontinued in favour of [TaskSwarm.js](https://github.com/Oxygem/TaskSwarm.js)**, which is based upon this.


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

##### Config.json

```json
{
    "share_key": "abc",
    "key": "test/tls/key.pem",
    "cert": "test/tls/certificate.pem",
    "redis": {
        "host": "localhost",
        "port": 6379
    },
    "host": "localhost",
    "port": 5000,
    "distributors": [
        ["localhost", 5000]
    ]
}
```

##### distributor.js

```js
#!/usr/bin/env node

var distributor = require('../distributed-task/distributor');
distributor.init(require('./config.json'));
```

##### worker.js

```js
#!/usr/bin/env node

var worker = require('../distributed-task/worker');
worker.init(require('./config.json'));
worker.addTask('name', someFunction);
```


## Full Example

A full example (almost deploy ready) can be found [here](./example).


## Definitions

#### Task

```js
{
    clean_end: true,      // allow distributors to clean up the task when it ends (& loose end status)
    state_optional: true; // allow the task to continue running when Redis goes down
                          // shouldn't happen but no guarantee the task won't also be requeued w/ multiple distributors-over-WAN
                          // it basically assumes all distributors share the same connection to Redis
}
```

#### Task Status

+ `RUNNING` - task is running, should have updated timestamp
+ `STOPPED` - task stopped intentionally by distributor/worker loosing related worker/distributor connection
+ `END` - task is complete, pending cleanup

#### Redis Bits

+ `new-task` - simple list based queue
+ `tasks` - set of all task_id's
+ `task-<uuid>` - individual task details

#### Failover/HA

+ A shorter loop is recommended for workers than distributors
+ All distributors/workers must have the same timezone set
+ Distributors/workers send health checks to each other according to a configured time interval
+ Both distributor and worker acknowledge disconnect on stream close + after missed health check
    * distributor flags worker as down and clears after interval, will requeue tasks
    * worker stops running tasks for this distributor (which should requeue them)
+ Upon disconnect from Redis: workers, for each connected distributor:
    * notify the distributor, which stops sending new task requests
    * pause running distributor tasks
    * check if we're connected to the distributor
    * if so, assumes distributor has also lost Redis connection & won't requeue
    * continue distributor tasks w/o `state_optional = true`
    * stop all other tasks from this distributor
+ Distributors watch for tasks w/ state `STOPPED` & `RUNNING` w/out of date timestamp, requeues
+ Same as above for `END` tasks with `clean_end = true`
+ Distributors watch their own tasks on a short interval, and all tasks on a long interval
+ Upon reconnect to Redis, workers check `state_optional` task timestamps before updating, and stop if newer one
