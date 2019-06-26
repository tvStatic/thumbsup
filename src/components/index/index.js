const _ = require('lodash')
const async = require('async')
const delta = require('./delta')
const EventEmitter = require('events')
const exiftool = require('../exiftool/parallel')
const fs = require('fs-extra')
const globber = require('./glob')
const moment = require('moment')
const path = require('path')
const warn = require('debug')('thumbsup:warn')

const EXIF_DATE_FORMAT = 'YYYY:MM:DD HH:mm:ssZ'

class Index {
  constructor (indexPath) {
    this.indexPath = indexPath

    // create the database if it doesn't exist
    fs.mkdirpSync(path.dirname(indexPath))

    this.initialise()
  }

  initialise () {
    var Database
    var sqlite3

    // better-sqlite3 may not be present - fallback to in memory processing
    try {
      Database = require('better-sqlite3')
      this.db = new Database(this.indexPath, {})
      this.usingBetterSQLite3 = true
    } catch (err) {
      // try sqlite3
      try {
        sqlite3 = require('sqlite3')
        this.db = new sqlite3.Database(this.indexPath)
        this.usingSQLite3 = true
        warn('better-sqlite3 is not available - using sqlite3 instead')
      } catch (err) {
        // use in memory map
        this.localDB = []
        this.usingInMemory = true
        warn('better-sqlite3 is not available - storing in memory instead')
      }
    }
  }

  /*
    Index all the files in <media> and store into <database>
  */
  update (mediaFolder, options = {}) {
    // will emit many different events
    const emitter = new EventEmitter()
    const index = this
    const createTableSQL = 'CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, timestamp INTEGER, metadata BLOB)'
    const selectStatementSQL = 'SELECT path, timestamp FROM files'
    const countStatementSQL = 'SELECT COUNT(*) AS count FROM files'
    const insertStatementSQL = 'INSERT OR REPLACE INTO files VALUES (?, ?, ?)'
    const deleteStatementSQL = 'DELETE FROM files WHERE path = ?'
    const selectMetadataSQL = 'SELECT * FROM files'
    const databaseMap = {}

    var initialise
    var emitRows
    var countRows
    var populateDatabaseMap
    var deleteRows
    var insertRow

    function useBetterSQLite () {
      var selectMetadata
      var countStatement
      var selectStatement
      var deleteStatement
      var insertStatement

      initialise = function (callback) {
        index.db.exec(createTableSQL)

        selectMetadata = index.db.prepare(selectMetadataSQL)
        countStatement = index.db.prepare(countStatementSQL)
        selectStatement = index.db.prepare(selectStatementSQL)
        deleteStatement = index.db.prepare(deleteStatementSQL)
        insertStatement = index.db.prepare(insertStatementSQL)

        callback()
      }

      emitRows = function (callback) {
        for (var row of selectMetadata.iterate()) {
          emitRow(row)
        }

        callback()
      }

      countRows = function (callback) {
        callback(countStatement.get().count)
      }

      populateDatabaseMap = function (callback) {
        for (var row of selectStatement.iterate()) {
          databaseMap[row.path] = row.timestamp
        }

        callback()
      }

      deleteRows = function (toDelete, callback) {
        _.each(toDelete, path => {
          deleteStatement.run(path)
        })

        callback()
      }

      insertRow = function (entry, timestamp, callback) {
        insertStatement.run(entry.SourceFile, timestamp, JSON.stringify(entry))
        callback()
      }
    }

    function useInMemory (callback) {
      initialise = function () {
        callback()
      }

      emitRows = function (callback) {
        index.localDB.forEach(emitRow)
        callback()
      }

      countRows = function (callback) {
        callback(index.localDB.length)
      }

      populateDatabaseMap = function (callback) {
        index.localDB.forEach(function (row) {
          databaseMap[row.path] = row.timestamp
        })

        callback()
      }

      deleteRows = function (toDelete, callback) {
        _.each(toDelete, path => {
          var foundIndex = index.localDB.findIndex(row => {
            return row.path === path
          })
          if (foundIndex !== -1) {
            index.localDB.splice(foundIndex, 1)
          }
        })

        callback()
      }

      insertRow = function (entry, timestamp, callback) {
        index.localDB.push({
          key: entry.SourceFile,
          timestamp: timestamp,
          metadata: JSON.stringify(entry)
        })

        callback()
      }
    }

    function useSQLite () {
      var selectMetadata
      var countStatement
      var deleteStatement
      var selectStatement
      var insertStatement

      initialise = function (callback) {
        index.db.serialize(function () {
          index.db.exec(createTableSQL)

          selectMetadata = index.db.prepare(selectMetadataSQL)
          countStatement = index.db.prepare(countStatementSQL)
          deleteStatement = index.db.prepare(deleteStatementSQL)
          selectStatement = index.db.prepare(selectStatementSQL)
          insertStatement = index.db.prepare(insertStatementSQL, callback)
        })
      }

      emitRows = function (callback) {
        index.db.serialize(function () {
          selectMetadata.each((err, row) => {
            emitRow(row)
          }, callback)
        })
      }

      countRows = function (callback) {
        var ret
        index.db.serialize(function () {
          countStatement.get((err, row) => {
            callback(row.count)
          })
        })
      }

      populateDatabaseMap = function (callback) {
        index.db.serialize(function () {
          selectStatement.each((err, row) => {
            databaseMap[row.path] = row.timestamp
          }, callback)
        })
      }

      deleteRows = function (toDelete, callback) {
        index.db.serialize(function () {
          var todo = []

          function deleteRow (path) {
            return function (callback) {
              deleteStatement.run(path, () => {
                callback()
              })
            }
          }

          _.each(toDelete, path => {
            todo.push(deleteRow(path))
          })

          async.series(todo, callback)
        })
      }

      insertRow = function (entry, timestamp, callback) {
        index.db.serialize(function () {
          insertStatement.run(entry.SourceFile, timestamp, JSON.stringify(entry), callback)
        })
      }
    }

    // emit every file in the index
    function emitRow (row) {
      emitter.emit('file', {
        path: row.path,
        timestamp: new Date(row.timestamp),
        metadata: JSON.parse(row.metadata)
      })
    }

    function init (callback) {
      if (index.usingBetterSQLite3) {
        useBetterSQLite()
      } else if (index.usingSQLite3) {
        useSQLite()
      } else {
        useInMemory()
      }

      initialise(callback)
    }

    function finished () {
      async.series([
        emitRows,
        function (callback) {
          // emit the final count
          countRows((result) => {
            emitter.emit('done', { count: result })
            callback()
          })
        }
      ])
    }

    async.series([
      init,
      // create hashmap of all files in the database
      function (callback) {
        // this is not set until init returns
        populateDatabaseMap(callback)
      }
    ],
    function () {
      // find all files on disk
      globber.find(mediaFolder, options, (err, diskMap) => {
        if (err) return console.error('error', err)

        // calculate the difference: which files have been added, modified, etc
        const deltaFiles = delta.calculate(databaseMap, diskMap)
        emitter.emit('stats', {
          unchanged: deltaFiles.unchanged.length,
          added: deltaFiles.added.length,
          modified: deltaFiles.modified.length,
          deleted: deltaFiles.deleted.length,
          total: Object.keys(diskMap).length
        })

        // remove deleted files from the DB
        deleteRows(deltaFiles.deleted)

        // check if any files need parsing
        var processed = 0
        const toProcess = _.union(deltaFiles.added, deltaFiles.modified)
        if (toProcess.length === 0) {
          return finished()
        }

        // call <exiftool> on added and modified files
        // and write each entry to the database
        const stream = exiftool.parse(mediaFolder, toProcess, options.concurrency)
        stream.on('data', entry => {
          const timestamp = moment(entry.File.FileModifyDate, EXIF_DATE_FORMAT).valueOf()

          insertRow(entry, timestamp)

          ++processed
          emitter.emit('progress', { path: entry.SourceFile, processed: processed, total: toProcess.length })
        }).on('end', finished)
      })
    })

    return emitter
  }

  /*
    Do a full vacuum to optimise the database
    which can be needed if files are often deleted/modified
  */
  vacuum () {
    if (this.db) {
      this.db.exec('VACUUM')
    }
  }
}

module.exports = Index
