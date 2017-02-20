//lets require/import the mongodb native drivers.
'use strict';
var elasticsearch = require('elasticsearch');
var mongodb = require('mongodb');
var configuration = require('./configuration');
//We need to work with "MongoClient" interface in order to connect to a mongodb server.
var MongoClient = mongodb.MongoClient;

// Connection URL. This is where your mongodb server is running.
var url = 'mongodb://' + configuration.mongo.username + ':' + configuration.mongo.password + '@' + configuration.mongo.url;

var elasticsearch = require('elasticsearch');
var elasticurl = '10.139.1.22:9200'
// var client = new elasticsearch.Client({
//     host: config.elastic.url
// });

var client = new elasticsearch.Client({
    hosts: configuration.elastic.hosts,
    connectionClass: require('http-aws-es'),
    amazonES: {
        region: configuration.elastic.region,
        accessKey: configuration.elastic.accessKey,
        secretKey: configuration.elastic.secretKey
    }
});

// Use connect method to connect to the Server
MongoClient.connect(url, function (err, db) {
    if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
    } else {
        //HURRAY!! We are connected. :)
        console.log('Connection established to', url);

        // Get the documents collection
        var collection = db.collection('liveht_content');
        var limit = 100;
        //We have a cursor now with our find criteria

        var date1 = new Date('2017.01.01').getTime() / 1000
        var date2 = new Date('2017.02.15').getTime() / 1000

        // var cursor = collection.find({}).batchSize(100);
        // var cursor = collection.find({
        //     "changed":
        //     {
        //         "$gte": date1,
        //         "$lt": date2
        //     }
        // }).batchSize(limit);
        var allresultscursor = collection.find({});
        allresultscursor.count().then((result) => {
            sendtoelastic(0);
        });

        function sendtoelastic(skip) {
            var cursor = collection.find({}).skip(skip).limit(limit);
            cursor.toArray(function (err, documents) {
                if (err) {
                    console.log("err:" + err);
                }
                // documents = documents.map(doc => {
                // console.log(documents);
                //     return doc;
                // })
                console.log("Mongo doc fetched:" + documents.length);
                console.log("total:" + skip);
                if (documents.length > 0) {
                    sendBulkQuery(documents, client, limit)
                        .then(() => {
                            sendtoelastic(skip + limit);
                        })
                        .catch((err) => {
                            console.log("elastic error" + err);
                        })
                }

            });

        }
    }
});


function sendBulkQuery(documents, client, limit) {
    //base case
    if (documents.length == 0) {
        return null;
    }
    var end;
    if (documents.length < limit) {
        end = documents.length;
    }
    else {
        end = limit;
    }
    var nextDocs = documents.splice(0, end);

    var bulkQueryBody = [];
    for (let index = 0; index < nextDocs.length; index++) {
        let doc = nextDocs[index];
        var type = doc.type;
        var config = { index: { _index: configuration.elasticindex.article, _type: type } };
        config['index']['_id'] = doc.nid;
        delete doc._id;
        bulkQueryBody.push(config)
        bulkQueryBody.push(doc);
    }

    // console.log(bulkQueryBody);
    return client.bulk({
        body: bulkQueryBody
    })
        .then(response => {
            console.log('Wrote ' + response.items.length + " documents");
            console.log('Time taken ' + response.took + " ms");
            console.log('');
            sendBulkQuery(documents, client, limit);

        })
        .catch((err) => {
            console.log("error on bulk query:" + err);
        })
}