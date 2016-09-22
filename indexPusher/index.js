'use strict';

const async = require('async'),
      azure = require('azure-storage');

const TILE_INDEX_CONTAINER = 'tileindexes2';
const PUSH_PARALLELISM = process.env.PUSH_PARALLELISM || 10;

let azureStorageEndpoint = process.env.LOCATION_STORAGE_ACCOUNT + ".blob.core.windows.net";

let totalAttempts = 0;
let totalFailures = 0;
let totalSuccesses = 0;
let functionsContext;

let retryOperations = new azure.ExponentialRetryPolicyFilter();
let blobService = azure.createBlobService(
    process.env.LOCATION_STORAGE_ACCOUNT,
    process.env.LOCATION_STORAGE_KEY,
    azureStorageEndpoint
).withFilter(retryOperations);

function pushResultSet(resultSetText, callback) {
    if (resultSetText.trim().length === 0) return callback();

    let successful = false;
    let resultSet;
    try {
        resultSet = JSON.parse(resultSetText);
    } catch(e) {
        functionsContext.log('failed to parse (skipping): ' + resultSetText);
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
               if (err) {
                   console.log(err);
               }

               if (!err) {
                   totalSuccesses += 1;
                   successful = true;
               } else {
                   totalFailures += 1;
               }

               totalAttempts += 1;

               if (totalAttempts % 500 === 0) {
                   functionsContext.log(`${functionsContext.invocationId}: ${totalAttempts}: ${totalSuccesses}/${totalFailures}`);
               }
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
    functionsContext = context;
    let resultSets = resultSetBlob.split('\n');
    if (timestamp) context.log('timestamp: ' + timestamp);
    if (timestamp) context.log('partCount: ' + partCount);
    pushResultSets(resultSets, err => {
        context.log('blob length: ' + resultSetBlob.length);
        context.log('err: ' + err);
        context.done(err);
    });
};
