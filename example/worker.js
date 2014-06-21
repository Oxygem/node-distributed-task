#!/usr/bin/env node

'use strict';

var fs = require('fs'),
    worker = require('../index').worker,
    config = require('./config/shared');

config.distributors = require('./config/distributors');


// Start the worker
worker.init(config);

// For each file in ./tasks load & add to worker
var files = fs.readdirSync('example/tasks');
for(var i=0; i<files.length; i++) {
    worker.addTask(files[i].replace('.js', ''), require('./tasks/' + files[i]));
}
