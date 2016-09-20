'use strict';

const fs = require('fs'),
      tileIndexPusher = require('./index.js');

let contextMock = {
    log: console.log,
    done: function(err) {
        console.log('done called: ' + err);
        process.exit(1);
    }
};

fs.readFile('part-00000', 'utf8', function (err, blobText) {
    if (err) return console.log(err);

    tileIndexPusher(contextMock, blobText);
});
