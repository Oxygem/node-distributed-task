// Distributed Task
// File: util.js
// Desc: utility functions!

'use strict';


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

    log: function(data) {
        if(this.debug) {
            var args = Array.prototype.slice.call(arguments, 1);
            if(args.length == 0) {
                console.log(this.log_prefix + data.toString());
            } else {
                args.unshift(this.log_prefix + data.toString());
                console.log.apply(console, args);
            }
        }
    },

    error: function(data) {
        var args = Array.prototype.slice.call(arguments, 1);
        if(args.length == 0) {
            console.log(this.log_prefix + data.toString());
        } else {
            args.unshift(this.log_prefix + data.toString());
            console.log.apply(console, args);
        }
    }
};
module.exports = util;
