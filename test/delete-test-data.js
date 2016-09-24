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

var query = `DELETE FROM maps WHERE data @> '{"test-data": true}'`
pool.query(query, function (err, pg_res) {
  if (err) 
    console.log(`failed to delete test data from msps err: ${err}`)
  else {
    console.log(`successfully deleted test data from maps`)
    var query = `DELETE FROM values WHERE entryData @> '{"test-data": true}'`
    pool.query(query, function (err, pg_res) {
      if (err) 
        console.log(`failed to delete test entryData from values err: ${err}`)
      else 
        console.log(`successfully deleted test data from values`)
      pool.end()
    })
  }
})