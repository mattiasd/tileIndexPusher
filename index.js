'use strict';

const async = require('async'),
      azure = require('azure-storage');

const TILE_INDEX_CONTAINER = 'tileindexes2';
const PUSH_PARALLELISM = 50;

let azureStorageEndpoint = process.env.LOCATION_STORAGE_ACCOUNT + ".blob.core.windows.net";

let retryOperations = new azure.ExponentialRetryPolicyFilter();
let blobService = azure.createBlobService(
    process.env.LOCATION_STORAGE_ACCOUNT,
    process.env.LOCATION_STORAGE_KEY,
    azureStorageEndpoint
).withFilter(retryOperations);

function pushResultSet(resultSetText, callback) {
    let successful = false;
    let resultSet;
    try {
        resultSet = JSON.parse(resultSetText);
    } catch(e) {
        console.error('skipping resultset that doesnt parse');
        return callback();
    }

    async.whilst(
        () => {
            return !successful;
        },
        createBlobCallback => {
           blobService.createBlockBlobFromText(TILE_INDEX_CONTAINER, resultSet.resultSetId, resultSetText, {
               cacheControl: "max-age=72000",
               contentType: "application/json"
           }, function(err) {
               if (err) console.log(err);
               if (!err) successful = true;

               createBlobCallback();
           });
        },
        callback
    );
}

function pushResultSets(resultSets, callback) {
    blobService.createContainerIfNotExists(TILE_INDEX_CONTAINER, {
        publicAccessLevel : 'blob'
    }, err => {
        if (err) return callback(err);

        async.eachLimit(resultSets, PUSH_PARALLELISM, pushResultSet, callback);
    });
}

module.exports = function(context, resultSetBlob) {
    console.log('this is a log message.');
    let resultSets = resultSetBlob.split('\n');
    pushResultSets(resultSets, err => {
        console.log('err: ' + err);
        context.log('err: ' + err);
        context.done();
    });
};
