const Spacebee = require('../../')
const Spacecore = require('bitspacecore')
const Spaceswarm = require('spaceswarm')

const db = new Spacebee(new Spacecore('./db'))
const swarm = new Spaceswarm()

swarm.on('connection', c => db.feed.replicate(c))

db.feed.ready().then(function () {
  console.log('Feed key: ' + db.feed.key.toString('hex'))
  swarm.join(db.feed.discoveryKey)
})

module.exports = db
