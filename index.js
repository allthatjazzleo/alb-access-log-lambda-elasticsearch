/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples
 * Sample code for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 *
 * Copyright 2015- Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
var AWS = require('aws-sdk');
var LineStream = require('byline').LineStream;
var zlib = require('zlib');
var readline = require('readline');
var parse = require('alb-log-parser'); 
var https = require('https');
var notHttps = require('http');
var http = process.env.HOST.includes('localhost')? notHttps : https
var path = require('path');
var stream = require('stream');
var indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, '');

/* Globals */
var esDomain = {
    endpoint: `${process.env.HOST || 'es.plutux.com'}`,
    region: 'ap-southeast-1',
    index: 'elblogs-' + indexTimestamp, // adds a timestamp to index. Example: elblogs-2016.03.31
    doctype: 'elb-access-logs',
    port: `${process.env.PORT || '443'}`,
    env: `${process.env.ENV || 'staging'}`
};
// var endpoint =  new AWS.Endpoint(esDomain.endpoint);
var s3 = new AWS.S3();
var totLogLines = 0;    // Total number of log lines in the file
var numDocsAdded = 0;   // Number of log lines added to ES so far
var sameIndex = '{ "index":{} }' // insert between every doc for bulk message to ES
var bulkNumber = `${process.env.BULK_NUMBER || 2000 }`

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
// var creds = new AWS.EnvironmentCredentials('AWS');

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3LogsToES(bucket, key, context, lineStream, recordStream) {
    // Note: The Lambda function should be configured to filter for .log files
    // (as part of the Event Source "suffix" setting).

    var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();

    // ungzip before read
    // var s3Stream = readline.createInterface({
    //     input: s3.getObject({Bucket: bucket, Key: key}).createReadStream().pipe(zlib.createGunzip())
    // });

    // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    s3Stream
        .pipe(zlib.createGunzip())
        .pipe(lineStream)
        .pipe(recordStream)
        .on('data', function(parsedEntry) {
            processDocTobulk(parsedEntry,false,context)
            // postDocumentToES(parsedEntry, context);
            // console.log(parsedEntry)
        })
        .on('end',function(parsedEntry){
            var done = true
            processDocTobulk(parsedEntry,done,context)
            // console.log(parsedEntry)
        });

    s3Stream.on('error', function() {
        console.log(
            'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
            'Make sure they exist and your bucket is in the same region as this function.');
        context.fail();
    });
}

// process doc to be bulk doc
var bulkDoc = []
function processDocTobulk(doc,done,context){
    if (!done) {
        var addedDoc = sameIndex+'\n'+doc
        bulkDoc.push(addedDoc)
        if (bulkDoc.length == bulkNumber) {
            numDocsAdded = numDocsAdded + parseInt(bulkNumber);
            var bulkDocMessage = bulkDoc.join('\n')+'\n'
            // console.log(bulkDocMessage)
            postDocumentToES(bulkDocMessage, context);
            bulkDoc = []
        }
    }else{
        numDocsAdded = numDocsAdded + parseInt(bulkDoc.length);
        var bulkDocMessage = bulkDoc.join('\n')+'\n'
        postDocumentToES(bulkDocMessage, context);
    }
}


/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
function postDocumentToES(doc, context) {
    // var req = new AWS.HttpRequest(endpoint);
    var post_options = {}
    post_options.method = 'POST';
    post_options.path = path.join('/', esDomain.index, esDomain.doctype,'_bulk');
    post_options.host = esDomain.endpoint;
    post_options.port = esDomain.port;
    // post_options.body = doc;
    post_options.headers = {'Content-Type': 'application/json'};

    // Set up the request to ES

    var post_req = http.request(post_options, function(res) {
        var body = '';
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            body += chunk;
        });
        res.on('end', function (chunk) {
            // console.log(body);
            console.log('numDocsAdded: '+numDocsAdded+'  vs  totalLogLines: '+totLogLines);
            if (numDocsAdded === totLogLines) {
                // Mark lambda success.  If not done so, it will be retried.
                console.log('All ' + numDocsAdded + ' log records added to ES.');
                // console.log('Last log added: '+doc)
                context.succeed();
            }
        })
    });
    post_req.write(doc);
    post_req.end();

    // // Sign the request (Sigv4)
    // var signer = new AWS.Signers.V4(req, 'es');
    // signer.addAuthorization(creds, new Date());

    // // Post document to ES
    // var send = new AWS.NodeHttpClient();
    // send.handleRequest(req, null, function(httpResp) {
    //     var body = '';
    //     httpResp.on('data', function (chunk) {
    //         body += chunk;
    //     });
    //     httpResp.on('end', function (chunk) {
    //         numDocsAdded ++;
    //         if (numDocsAdded === totLogLines) {
    //             // Mark lambda success.  If not done so, it will be retried.
    //             console.log('All ' + numDocsAdded + ' log records added to ES.');
    //             context.succeed();
    //         }
    //     });
    // }, function(err) {
    //     console.log('Error: ' + err);
    //     console.log(numDocsAdded + 'of ' + totLogLines + ' log records added to ES.');
    //     context.fail();
    // });
}

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log('Received event: ', JSON.stringify(event, null, 2));
    
    /* == Streams ==
    * To avoid loading an entire (typically large) log file into memory,
    * this is implemented as a pipeline of filters, streaming log data
    * from S3 to ES.
    * Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    */
    var lineStream = new LineStream();
    // A stream of log records, from parsing each log line
    var recordStream = new stream.Transform({objectMode: true})
    recordStream._transform = function(line, encoding, done) {
        var logRecord = parse(line.toString());
        logRecord.env = esDomain.env
        var serializedRecord = JSON.stringify(logRecord);
        this.push(serializedRecord);
        totLogLines ++;
        done();
    }

    event.Records.forEach(function(record) {
        var bucket = record.s3.bucket.name;
        var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
        s3LogsToES(bucket, objKey, context, lineStream, recordStream);
    });
}