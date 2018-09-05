const config = require('../config/config')
const web3config = config.web3.networks[config.web3.network]
const MDAPPArtifact = require('../contracts/MDAPP')
const { utils } = require('./utils')
const Web3 = require('web3')


const app = {
  isConnected: false,

  start () {
    this.initWeb3()
    this.maintainConnection()
    this.findOrphanedFiles()
  },

  initWeb3 () {
    console.log(`Establishing connection to ${web3config.provider}`)
    this.provider = new Web3.providers.WebsocketProvider(web3config.provider)
    this.provider.on('connect', () => {
      this.isConnected = true
      console.log(`connected to ${web3config.provider}`)

      // Start listening for events.
      this.watch()
    })
    this.provider.on('end', async (e)  => {
      // In dev environment this means the blockchain has been reset.
      if (config.env === 'dev') {
        await utils.removeVariable('lastEventBlock')
      }

      this.isConnected = false
      console.log('connection lost, reconnect...')
      setTimeout(() => {
        this.initWeb3()
      }, 5000)
    })

    this.web3 = new Web3(this.provider)
    this.mdapp = new this.web3.eth.Contract(MDAPPArtifact.abi, MDAPPArtifact.networks[config.web3.network].address)
  },

  async watch () {
    if (this.isConnected) {
      try {
        let startBlock = await utils.getVariable('lastEventBlock')
        startBlock = startBlock ? parseInt(startBlock) + 1 : web3config.dappGenesis
        console.log('begin watching at block:', startBlock)

        this.mdapp.events.EditAd({fromBlock: startBlock, toBlock: 'latest'})
          .on('data', async (event) => {
            let r = event.returnValues
            try {
              let hash = utils.multihash2hash(r.hashFunction, r.digest)
              await utils.addToIPFS(hash)
              utils.removeHashFromDynamoDb(hash)

              // If this script was down while multiple events emitted, the execution order is not guaranteed.
              if (event.blockNumber > startBlock) {
                startBlock = event.blockNumber
                utils.setVariable('lastEventBlock', event.blockNumber)
              }

              utils.removeFromUploadDir(hash)
              console.log(`processed file ${hash} at block ${event.blockNumber}`)
            } catch (e) {
              console.error(e)
            }
          })
          .on('changed', event => {
            console.log('event removed from blockchain:', event)
          })
          .on('error', error => {
            console.error(error)
          })
      } catch (e) {
        console.error(e)
      }
    }
  },

  /**
   * Send a packet to the server to maintain an idle connection.
   */
  async maintainConnection () {
    if (this.isConnected) {
      try {
        await this.web3.eth.net.isListening()
      } catch (e) {}
    }
    setTimeout(() => { this.maintainConnection() }, 20000)
  },

  async findOrphanedFiles () {
    if (this.isConnected) {
      try {
        let expired = await utils.getExpiredFiles(config.expireAfterSeconds)
        if (expired.length) {
          let mhs = new Map()
          let digests =[]
          for (let i = 0; i < expired.length; i++) {
            let mh = utils.hash2multihash(expired[i])
            mhs.set(mh.digest, mh)
            digests.push(mh.digest)
          }

          // Double check if those hashes really really aren't mined into blockchain. This can happen if we miss an
          // EditAd event above (server glitch or whatever).
          // Look for EditAd events with the expired multihashes.
          this.mdapp.getPastEvents('EditAd', {
            filter: {digest: digests},
            fromBlock: web3config.dappGenesis,
            toBlock: 'latest'
          })
          .then(async (events) => {
            // We found events with equal digest. Do hashFunction and size also fit?
            try {
              for (let i = 0; i < events.length; i++) {
                let r = events[i].returnValues
                let mh = mhs.get(r.digest)

                if (r.hashFunction === mh.hashFunction && parseInt(r.size) === mh.size) {
                  // Phew! This file MUST NOT be deleted.
                  console.log('do not delete:', hash)

                  // Move file to save harbour
                  let hash = utils.multihash2hash(mh.hashFunction, mh.digest)
                  await utils.addToIPFS(hash)
                  utils.removeHashFromDynamoDb(hash)
                  utils.removeFromUploadDir(hash)

                  // Remove from multihash map
                  mhs.delete(mh.digest)
                }
              }
            } catch (e) {
              console.error(e)
            }
          })

          // All remaining multihashes can be deleted.
          mhs.forEach(mh => {
            let hash = utils.multihash2hash(mh.hashFunction, mh.digest)
            console.log('delete:', hash)
            utils.removeHashFromDynamoDb(hash)
            utils.removeFromS3(hash)
            utils.removeFromUploadDir(hash)
          })
        }
      } catch (e) {
        console.error(e)
      }
    }
    setTimeout(() => { this.findOrphanedFiles() }, 5000)
  }
}

app.start()
