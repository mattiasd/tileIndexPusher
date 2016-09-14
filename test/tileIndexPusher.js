"use strict"

let assert = require('assert'),
    tilePusherFunction = require('../index');

describe('tileIndexPusher service', () => {
    it('can push a resultsets blob', done => {

        let resultSetBlob =
`{"activities": [{"d": 11839.7, "g": 99, "l": 107, "s": 1698, "bbox": {"west": -122.32065, "east": -122.27616, "north": 37.548267, "south": 37.512276}, "td": 5935, "id": "x212636364"}], "resultSetId": "everyone-16_25384_10503"}
{"activities": [{"d": 15209.4, "g": 311, "l": 268, "s": 3096, "bbox": {"west": -121.984, "east": -121.90453, "north": 37.1456, "south": 37.107723}, "td": 3361, "id": "x212635803"}], "resultSetId": "everyone-17_50966_21148"}
{"activities": [{"d": 17347, "g": 32, "l": 31, "s": 405, "bbox": {"west": -77.167496, "east": -77.12163, "north": 38.79473, "south": 38.740433}, "td": 6495, "id": "x208203962"}], "resultSetId": "everyone-17_50193_37455"}
`;

        let context = {
            log: msg => console.log,
            done: err => {
                assert(!err);
                done();
            }
        };

        tilePusherFunction(context, resultSetBlob);
    });
});