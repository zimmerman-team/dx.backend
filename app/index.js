// Imports
const path = require('path');
const fs = require('fs');
// Project
const processDataset = require('./processDataset.js').processDataset;
const createServiceFile = require('./apiBuilder.js').createServiceFile;
// Constants
const modelFile = path.join(__dirname, '../staging/db/schema.cds');
const dataFolder = path.join(__dirname, '../staging/db/data');

module.exports = {onLoad: async function() {
    // Gather the names of the datasources within the datasource folder
    let sources = [];
    fs.readdirSync(dataFolder).forEach(file => { sources.push(file) });

    // create an empty string which will contain the models to be appended to the model file
    console.debug("ONLOAD::Preparing data model for new source...");
    for (let i in sources) {
        const filepath = path.join(__dirname, `../staging/db/data/${sources[i]}`);
        // if the file is a directory, skip
        if (fs.statSync(filepath).isDirectory()) continue;
        const name = path.parse(sources[i]).name.replace('data-', '');
        addDataScraper(name);
        // if the name is already in the data-service file, skip.
        if (fs.readFileSync(modelFile, 'utf8').includes(name)) {
            continue;
        };
        await processDataset(filepath, name, sources[i]);
    }
    createServiceFile(); // make sure the service file is up to date with the latest models.
    console.debug("Ready to serve your data at http://localhost:4004/");
}}

function addDataScraper(name) {
    // Update SSR with an additional dataset for the SSR parser to process.
    // load the additional datasets json list
    const datascraperDatasetsLoc = process.env.DATA_EXPLORER_SSR + "additionalDatasets.json";
    let additionalDatasets = JSON.parse(fs.readFileSync(datascraperDatasetsLoc, 'utf8'));
    // check if the name is already in the ids of the loaded list
    for (const dataset of additionalDatasets) {
        if (dataset.id === name.substring(2)) return;
    }
    const dsObj = {
        "id": name.substring(2),
        "url": "http://localhost:4004/data/" + name + "?",
        "dataPath": "value",
        "countUrl": "http://localhost:4004/data/" + name + "?$top=0&$count=true",
        "countPath": "@odata.count"
    };
    additionalDatasets.push(dsObj);
    fs.writeFileSync(datascraperDatasetsLoc, JSON.stringify(additionalDatasets, null, 2));
}
