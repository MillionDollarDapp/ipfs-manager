const config = require('../config/config')
const web3config = config.web3.networks[config.web3.network]
const MDAPPArtifact = require('../contracts/MDAPP')

const { utils } = require('./utils')

const Web3 = require('web3')
const web3 = new Web3(new Web3.providers.WebsocketProvider(web3config.provider))

const mdapp = new web3.eth.Contract(MDAPPArtifact.abi, MDAPPArtifact.networks[config.web3.network].address)


// Start watching for EditAd events.
let editAdWatcher = mdapp.events.EditAd({fromBlock: web3config.fromBlock})
  .on('data', async (event) => {
    let r = event.returnValues
    try {
      let hash = utils.multihash2hash(r.hashFunction, r.digest, r.size, r.storageEngine)
      await Promise.all([utils.addToIPFS(hash), utils.removeHashFromDynamoDb(hash), utils.setVariable('lastEventBlock', event.blockNumber)])
      utils.removeFromUploadDir(hash)
    } catch (e) {
      console.error(e)
    }
  })
  .on('error', error => {
    console.error(error)
  })

// TODO:
// Check if web3.eth.isListening still true - otherwise resubscribe

// TODO:
// Mark file as deletable from S3 and disk if Date.now() - timestamp > x hours (check every x min)


