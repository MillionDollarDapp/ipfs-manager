const config = require('../config/config')
const web3config = config.web3.networks[config.web3.network]
const MDAPPArtifact = require('../contracts/MDAPP')
const { utils } = require('./utils')
const Web3 = require('web3')


const app = {
  isConnected: false,

  start () {
    this.initWeb3()
    this.mdapp = new this.web3.eth.Contract(MDAPPArtifact.abi, MDAPPArtifact.networks[config.web3.network].address)
    this.maintainConnection()
  },

  initWeb3 () {
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
      }, 500)
    })

    this.web3 = new Web3(this.provider)
  },

  async watch () {
    try {
      let startBlock = await utils.getVariable('lastEventBlock')
      startBlock = startBlock ? parseInt(startBlock) + 1 : web3config.fromBlock
      console.log('begin at block:', startBlock)

      this.mdapp.events.EditAd({fromBlock: startBlock, toBlock: 'latest'})
        .on('data', async (event) => {
          let r = event.returnValues
          try {
            let hash = utils.multihash2hash(r.hashFunction, r.digest, r.size, r.storageEngine)
            await Promise.all([utils.addToIPFS(hash), utils.removeHashFromDynamoDb(hash)])

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
          console.log('something changed')
        })
        .on('error', error => {
          console.error(error)
        })
    } catch (e) {
      console.error(e)
    }
  },

  /**
   * Send a packet to the server to maintain an idle connection.
   */
  async maintainConnection () {
    try {
      await this.web3.eth.net.isListening()
    } catch (e) {}
    setTimeout(() => { this.maintainConnection() }, 10000)
  }
}

app.start()

// TODO:
// Mark file as deletable from S3 and disk if Date.now() - timestamp > x hours (check every x min)


