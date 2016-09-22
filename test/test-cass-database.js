const database = require('../maps-cass.js');
const uuidgen = require('node-uuid')


var namespace = 'acme'
var uuid = uuidgen.v4()
var name = 'nursery-rhymes' + Math.random()
var key = 'HumptyDumpty'
var contentType = 'text/plain'
var text = 'Humpty Dumpty sat on a wall\
Humpty Dumpty had a great fall'
const value = new Buffer(text, 'utf-8')
const cleanup = false;

var map = {
  namespace: namespace,
  name: name
}

var patchedMap = {
  namespace: namespace+'-patched',
  name: name+'-patched'
}

var entry = {
  something: 'a value'
}

var metadata = {
  contentType: contentType
}
//TODO get some better tests or make sure API tests it well
database.init(function (err) {
  if (err)
    console.error(err)
  else {
    database.createMapThen(uuid, map, function (err, etag) {
      console.log('new map etag = ' + etag)
      database.updateMapThen(uuid, patchedMap, function(err, etag){
        console.log('patched map = '+JSON.stringify(patchedMap))
      database.getMapsFromNamespace(patchedMap.namespace ? patchedMap.namespace : map.namespace, function (err, maps) {
        console.log('maps from namespace = ' + JSON.stringify(maps))
        database.createEntryThen(uuid, key, entry, function (err, etag) {
          console.log('new map entry etag = ' + etag)
          database.upsertValueThen(uuid, key, metadata, value, function (err, etag) {
            console.log('new map value etag = ' + etag)
            database.getMapId(patchedMap.namespace ? patchedMap.namespace : '', patchedMap.name, function (err, uuid) {
              console.log('map uuid = ' + uuid)
              var returnedUuid = uuid
              database.withMapByNameDo(patchedMap.namespace ? patchedMap.namespace : '', patchedMap.name, function (err, map, etag) {
                console.log('map = '+JSON.stringify(map))
                var returnedMap = map
                returnedMap['newField'] = 'newValue'
                database.updateMapThen(returnedUuid, returnedMap, function (err, etag) {
                  console.log('updated map etag = ' + etag)
                  database.withEntriesDo(returnedUuid, function (err, entries) {
                    console.log('entry list = ' + JSON.stringify(entries))
                    database.getMapEntry(returnedUuid, key, function (err, key, data, etag) {
                      console.log('single entry key = ' +key+ ', data= '+JSON.stringify(data), ', etag = '+etag)
                      database.withValueDo(returnedUuid, key, function (err, value, type, etag) {
                        console.log('map value = ' + JSON.stringify(value) + ', type = '+type+', etag = '+etag)
                        if (!cleanup)
                          process.exit(0)
                        console.log('deleting map')
                        database.deleteMapThen(returnedUuid, function (err) {
                          if (err)
                            console.error(err)
                          else
                            process.exit(0)
                        })
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })
  })
  }
})










