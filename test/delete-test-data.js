'use strict';
var Pool = require('pg').Pool;

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
};

console.log(`start delete test data`)
var pool = new Pool(config);

function deleteTestDataThen(table, callback) {
  var query = `DELETE FROM ${table} WHERE data @> '{"test-data": true}'`;
  pool.query(query, function (err, pg_res) {
    if (err) 
      console.log(`failed to delete test data from ${table}`)
    else
      console.log(`successfully deleted test data from ${table}`)      
    callback()
  });
}

deleteTestDataThen('maps', function(){
  deleteTestDataThen('entries', function() {
    deleteTestDataThen('values', function() {
      pool.end()
    })
  })
})