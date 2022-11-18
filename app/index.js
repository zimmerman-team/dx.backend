// Imports
// import path from 'path';
// import fs from 'fs';
// import { fileURLToPath } from 'url';
// // Project
// import { processDataset } from './processDataset.js';
// import { createServiceFile } from './apiBuilder.js';
// import { generateConfigs } from './configBuilder.js';
const path = require('path')
const fs = require('fs')
// const fileURLToPath = require('url').fileURLToPath

const processDataset = require('./processDataset.js').processDataset;
const createServiceFile = require('./apiBuilder.js').createServiceFile;

// Constants
// const __dirname = path.dirname(fileURLToPath(import.meta.url));
const modelFile = path.join(__dirname, '../staging/db/schema.cds');
const dataFolder = path.join(__dirname, '../staging/db/data');

console.log(modelFile,"\n", dataFolder, "\nHELLOOOOOOOOOOO")

module.exports = {onLoad: async function() {
    // Gather the names of the datasources within the datasource folder
    let sources = [];
    fs.readdirSync(dataFolder).forEach(file => { sources.push(file) });

    // create an empty string which will contain the models to be appended to the model file
    console.debug("Preparing data models...");
    let appendString = ``;
    for (let i in sources) {
        const filepath = path.join(__dirname, `../staging/db/data/${sources[i]}`);
        // if the file is a directory, skip
        if (fs.statSync(filepath).isDirectory()) continue;
        const name = path.parse(sources[i]).name.replace('data-', '');

        // Generate empty configs for the data source if they don't exist
        // generateConfigs(name);
        addDataScraper(name)
        // if the name is already in the data-service file, skip.
        if (fs.readFileSync(modelFile, 'utf8').includes(name)) {
            continue
        };
        appendString = await processDataset(filepath, name, appendString, sources[i]);
    }

    // write the new model to the model file.
    if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
        fs.appendFileSync(modelFile, appendString);
    }
    createServiceFile(); // make sure the service file is up to date with the latest models.
    console.debug("Ready to serve your data at http://localhost:4004/");
}}

function addDataScraper(name) {
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
    fs.writeFileSync(datascraperDatasetsLoc, JSON.stringify(additionalDatasets, null, 2))
}
