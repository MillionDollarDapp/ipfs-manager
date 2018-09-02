const config = require('../config/config')
const fs = require('fs')
const multihashes = require('multihashes')
const Web3 = require('web3')
const web3 = new Web3() // No provider needed here

const AWS = require('aws-sdk')
if (config.env === "dev") {
  AWS.config.update(config.aws.dev)
} else {
  AWS.config.update(config.aws.live)
}

const ddb = new AWS.DynamoDB(config.dynamoDb.all)

const ipfsAPI = require('ipfs-api')
const ipfs = ipfsAPI()

const utils = {
  addToIPFS (hash) {
    return new Promise((resolve, reject) => {
      let path = `${config.uploadDir}/${hash}`
      if (fs.existsSync(path)) {
        // Add to ipfs
        fs.readFile(path, (err, data) => {
          if (err) reject(err)
          ipfs.files.add(data, function (err, files) {
            if (err) reject(err)
            if (files[0].hash !== hash) reject(new Error(`Wrong hash: ${path} now has ${files[0].hash}`))
            resolve()
          })
        })
      } else {
        reject(new Error(`File ${path} doesn't exist.`))
      }
    })
  },

  removeFromUploadDir (hash) {
    let path = `${config.uploadDir}/${hash}`
    if (fs.existsSync(path)) {
      fs.unlinkSync(path)
    }
  },

  removeHashFromDynamoDb (hash) {
    return new Promise((resolve, reject) => {
      const params = {
        TableName: config.dynamoDb.table_files,
        Key: {
          hash: {'S': hash},
        }
      }

      ddb.deleteItem(params, function(err) {
        if (err) reject(err)
        resolve()
      })
    })
  },

  markHashExpired (hash) {
    return new Promise((resolve, reject) => {
      const params = {
        TableName: config.dynamoDb.table_files,
        Key: {
          'hash': {'S': hash}
        },
        AttributeUpdates: {
          'expired': {
            Action: 'PUT',
            Value: {'BOOL': true}
          }
        }
      }

      ddb.updateItem(params, function(err) {
        if (err) reject(err)
        resolve()
      })
    })
  },

  setVariable (name, value) {
    return new Promise((resolve, reject) => {
      value = typeof value === 'String' ? value : value.toString()

      const params = {
        TableName: config.dynamoDb.table_variables,
        Item: {
          name: {'S': name},
          value: {'S': value},
        }
      }

      ddb.putItem(params, function(err) {
        if (err) reject(err)
        resolve()
      })
    })
  },

  getVariable (name) {
    return new Promise((resolve, reject) => {
      const params = {
        TableName: config.dynamoDb.table_variables,
        Key: {
          name: {'S': name}
        }
      }

      ddb.getItem(params, function(err) {
        if (err) reject(err)
        resolve()
      })
    })
  },

  multihash2hash (hashFunction, digest, size, storageEngine) {
    storageEngine = web3.utils.hexToAscii(storageEngine)

    if (storageEngine === 'ipfs') {
      hashFunction = hashFunction.substr(2)
      digest = digest.substr(2)
      return multihashes.toB58String(multihashes.fromHexString(hashFunction + digest))
    }

    throw new Error('Unknown storage engine:', storageEngine)
  }
}

module.exports = { utils: utils }