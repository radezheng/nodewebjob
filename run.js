'use strict';

var EventHubClient = require('azure-event-hubs').Client;

var connectionString = process.env.IOTHUB_CONN;

console.log(connectionString);

// get the client
const mysql = require('mysql2');

const connection = mysql.createConnection({
  host: process.env.mysqlhost,
  user: process.env.mysqlusr,
  password: process.env.mysqlpwd,
  database: process.env.mysqldb,
  port: process.env.mysqlport,
  ssl: true
});

connection.connect();

var printError = function (err) {
  console.log(err.message);
};

var handleMySQLResult = function(err, results, fields){
    console.log(err);
    console.log(results); // results contains rows returned by server
    console.log(fields); // fields contains extra meta data about results, if available

};

var printMessage = function (message) {
  console.log('Message received: ');
  
  console.log(JSON.stringify(message.body));
  console.log(JSON.stringify(message.applicationProperties));
  connection.execute(
  'insert into tbltest values(?, ?)',
  [message.body.temperature, new Date()],
    handleMySQLResult
);
  console.log('');
};

var client = EventHubClient.fromConnectionString(connectionString);
client.open()
    .then(client.getPartitionIds.bind(client))
    .then(function (partitionIds) {
        return partitionIds.map(function (partitionId) {
            return client.createReceiver('$Default', partitionId, { 'startAfterTime' : Date.now()}).then(function(receiver) {
                console.log('Created partition receiver: ' + partitionId)
                receiver.on('errorReceived', printError);
                receiver.on('message', printMessage);
            });
        });
    })
    .catch(printError);

