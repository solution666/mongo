const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb+srv://<username>:<password>@<clustername>.mongodb.net/<dbname>?retryWrites=true&w=majority";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
client.connect(err => {
  const collection = client.db("<dbname>").collection("<collectionname>");
  // perform actions on the collection object
  client.close();
});

const fs = require('fs');
const https = require('https');

const file = fs.createWriteStream("movies_1.gz");
const request = https.get("https://example.com/movies_1.gz", function(response) {
  response.pipe(file);
});

const unzipper = require('unzipper');

fs.createReadStream('movies_1.gz')
  .pipe(unzipper.Parse())
  .on('entry', function (entry) {
    const fileName = entry.path;
    if (fileName.endsWith('.json')) {
      entry.pipe(fs.createWriteStream(fileName));
    } else {
      entry.autodrain();
    }
  });

const readline = require('readline');
const rl = readline.createInterface({
  input: fs.createReadStream('movies.json')
});

rl.on('line', function (line) {
  const movie = JSON.parse(line);
  collection.insertOne(movie, function(err, res) {
    if (err) throw err;
    console.log("Document inserted");
  });
});
