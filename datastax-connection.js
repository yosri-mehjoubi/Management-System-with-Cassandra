const { Client } = require('datastax-driver');

const config = require('./datastax-config.json'); // Charger les informations d'identification

const client = new Client({
  cloud: {
    secureConnectBundle: 'datastax-config.json', // Chemin vers le bundle de connexion sécurisée
    credentials: {
      clientId: config.clientId,
      clientSecret: config.secret,
    },
  },
});

// Utilisez le client pour interagir avec votre cluster
// Par exemple : client.execute('SELECT * FROM keyspace.table;')
