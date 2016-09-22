'use strict';

const async = require('async'),
      azure = require('azure-storage');

const TILE_INDEX_CONTAINER = 'tileindexes2';
const PUSH_PARALLELISM = process.env.PUSH_PARALLELISM || 10;

let azureStorageEndpoint = process.env.LOCATION_STORAGE_ACCOUNT + ".blob.core.windows.net";

let blobService = azure.createBlobService(
    process.env.LOCATION_STORAGE_ACCOUNT,
    process.env.LOCATION_STORAGE_KEY,
    azureStorageEndpoint
).withFilter(new azure.ExponentialRetryPolicyFilter());

function pushResultSet(resultSetText, callback) {
    if (resultSetText.trim().length === 0) return callback();

    let successful = false;
    let resultSet;
    try {
        resultSet = JSON.parse(resultSetText);
    } catch(e) {
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
                   successful = true;
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
    let resultSets = resultSetBlob.split('\n');
    pushResultSets(resultSets, err => {
        context.log('blob length: ' + resultSetBlob.length);
        context.log('err: ' + err);
        context.done(err);
    });
};
