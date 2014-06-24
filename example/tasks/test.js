'use strict';

var events = require('events'),
    util = require('util');

module.exports = function(manager, data) {
    events.EventEmitter.call(this);
    var self = this;

    // This task echos timeout! after one second & stops, to be cleaned up
    var end = setTimeout(function() {
        manager.log('timeout!');

        // Notify task finished
        self.emit('_end');
    }, 1000);


    // Stop when requested
    manager.on('stop', function() {
        // Stop task
        clearTimeout(end);

        // Notify stopped
        self.emit('_stop');
    });
};
util.inherits(module.exports, events.EventEmitter);
