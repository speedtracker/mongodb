const MongoClient = require('mongodb').MongoClient

const MongoDBConnector = function ({connectionString, database}) {
  this.connectionString = connectionString
  this.database = database
}

MongoDBConnector.prototype.connect = function () {
  return new Promise((resolve, reject) => {
    MongoClient.connect(this.connectionString, (err, client) => {
      if (err) return reject(err)

      this.client = client
      this.db = client.db(this.database)

      resolve(this.db)
    })    
  })
}

MongoDBConnector.prototype.disconnect = function () {
  this.client.close()
}

MongoDBConnector.prototype.get = function ({
  collection,
  timestampFrom,
  timestampTo
}) {
  let options = {}

  if (typeof timestampFrom === 'number') {
    options.timestamp = {
      $gte: timestampFrom
    }
  }

  if (typeof timestampTo === 'number') {
    options.timestamp = options.timestamp || {}
    options.timestamp.$lte = timestampTo
  }

  return new Promise((resolve, reject) => {
    this.db.collection(collection)
      .find(options)
      .sort({timestamp: 1})
      .toArray((err, results) => {
        if (err) return reject(err)

        resolve(results)
      })
  })
}

MongoDBConnector.prototype.insert = function ({collection, results}) {
  return new Promise((resolve, reject) => {
    this.db.collection(collection).insertMany(results, (err, response) => {
      if (err) return reject(err)

      resolve(response)
    })
  })   
}

module.exports = MongoDBConnector
