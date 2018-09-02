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

  getExpiredFiles (ttl) {
    return new Promise((resolve, reject) => {
      let beforeTimestamp = Math.floor(Date.now() / 1000) - ttl

      const params = {
        TableName: config.dynamoDb.table_files,
        IndexName: 'uploaded-index',
        FilterExpression: '#uploaded <= :value',
        ExpressionAttributeNames: {
          '#uploaded': 'uploaded'
        },
        ExpressionAttributeValues: {
          ':value': { 'N': beforeTimestamp.toString()}
        },
        Select: 'ALL_PROJECTED_ATTRIBUTES',
        ReturnConsumedCapacity: 'NONE'
      }

      ddb.scan(params, function(err, data) {
        if (err) reject(err)

        if (data && data.hasOwnProperty('Items')) {
          let items = []
          data.Items.forEach(item => {
            items.push(item.hash.S)
          })
          resolve(items)
        } else {
          resolve(null)
        }
      })
    })
  },

  removeFromS3 (hash) {
    return new Promise((resolve, reject) => {
      let params = {
        Bucket: config.s3.bucket,
        Key: hash
      }

      let s3config = config.s3.all
      if (config.env === "dev") s3config.endpoint = config.s3.dev.endpoint

      let s3 = new AWS.S3(s3config);
      s3.deleteObject(params, function (err) {
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

      ddb.getItem(params, function(err, data) {
        if (err) reject(err)

        if (data.hasOwnProperty('Item')) {
          resolve(data.Item.value.S)
        } else {
          resolve(null)
        }
      })
    })
  },

  removeVariable (name) {
    return new Promise((resolve, reject) => {
      const params = {
        TableName: config.dynamoDb.table_variables,
        Key: {
          name: {'S': name},
        }
      }

      ddb.deleteItem(params, function(err) {
        if (err) reject(err)
        resolve()
      })
    })
  },

  multihash2hash (hashFunction, digest) {
    hashFunction = hashFunction.substr(2)
    digest = digest.substr(2)
    return multihashes.toB58String(multihashes.fromHexString(hashFunction + digest))
  },

  hash2multihash (hash) {
    let mh = multihashes.fromB58String(Buffer.from(hash))
    return {
      hashFunction: '0x' + mh.slice(0, 2).toString('hex'),
      digest: '0x' + mh.slice(2).toString('hex'),
      size: mh.length - 2
    }
  }
}

module.exports = { utils: utils }