#!/usr/bin/env node

'use strict';

var distributor = require('../index').distributor,
    config = require('./config/shared');

config.host = 'localhost';
config.port = 5000;


// Start distributor
distributor.init(config);
