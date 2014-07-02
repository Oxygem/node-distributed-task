// Distributed Task
// File: util.js
// Desc: utility functions!

'use strict';


var colors,
    red,
    blue,
    green;
try {
    colors = require('colors'),
    red = function(str) { return str.red; },
    blue = function(str) { return str.blue; },
    green = function(str) { return str.green; };
} catch(e) {
    red = blue = green = function(str) { return str; }
}


var util = {
    receiveUntil: function(stream, want, callback, options) {
        var buffer = '';

        if(options.timeout) {
            var timeout = setTimeout(function() {
                stream.end();
            }, options.timeout * 1000);
        }

        var onData = function(data) {
            buffer += data.toString();
            if(buffer.length >= want.length) {
                if(buffer == want) {
                    if(options.timeout)
                        clearTimeout(timeout);

                    stream.removeListener('data', onData);
                    callback(stream);
                } else {
                    // Rejected!
                    stream.end();
                }
            }
        }
        stream.on('data', onData);
    },

    log: function(action, data) {
        if(this.debug) {
            data = data || '';
            var args = Array.prototype.slice.call(arguments, 2);
            if(args.length == 0) {
                console.log(green(this.log_prefix) + blue(action) + ' ' + data.toString());
            } else {
                args.unshift(green(this.log_prefix) + blue(action) + ' ' + data.toString());
                console.log.apply(console, args);
            }
        }
    },

    error: function(data) {
        var args = Array.prototype.slice.call(arguments, 1);
        if(args.length == 0) {
            console.error(red(this.log_prefix + data.toString()));
        } else {
            args.unshift(red(this.log_prefix + data.toString()));
            console.error.apply(console, args);
        }
    }
};
module.exports = util;
