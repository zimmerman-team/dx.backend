// Imports
const path = require('path')
const fs = require('fs')
// import path from 'path';
// import fs from 'fs';
// import { fileURLToPath } from 'url';
// Project
// import { getMostCommonFieldTypes } from './dataFieldTypes.js';
const getMostCommonFieldTypes = require('./dataFieldTypes').getMostCommonFieldTypes
// Constants
// const __dirname = path.dirname(fileURLToPath(import.meta.url));
const serviceFile = path.join(__dirname, '../staging/srv/data-service.cds');
const modelFile = path.join(__dirname, '../staging/db/schema.cds');

// Create a service file for each of the models available in the data/schema.cds folder.
module.exports = {
createServiceFile: function() {
    console.debug("SERVICEFILE::Preparing data services...");
    let writeStr = `using { data as my } from '../db/schema';\nservice CatalogService @(path:'/data') {\n`;

    // get the name from each created model, found in the model file
    let modelFileContent = fs.readFileSync(modelFile, 'utf8');
    let modelIndexes = modelFileContent.match(/entity ([a-zA-Z0-9]+) : managed/g);
    for (let i in modelIndexes) {
        // remove surrounding cds tags from model name and create a service string
        const name = modelIndexes[i].replace('entity ', '').replace(' : managed', '');
        writeStr += `\t@readonly entity ${name} as SELECT from my.${name} {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };\n`;
    }
    writeStr += `}\n`; // close the new service file string
    if (!fs.readFileSync(serviceFile, 'utf8').includes(writeStr)) {
        fs.writeFileSync(serviceFile, writeStr);
    }
},

// Create a model for the data source
createModelFile: function(data, name) {
    const allFields = getMostCommonFieldTypes(data);
    // create the entity header
    let res = `\nentity ${name} : managed {\n\t`;
    // check if an ID is present within the data source, if not, add a default ID field
    let idFound = false;
    for (let key in allFields) {
        if (key.toLowerCase() == 'id') {
            idFound = true;
            break;
        }
    }
    if (!idFound) res += `key ID : Integer;\n`;

    // add each key with its type to the entity
    for (let key in allFields) {
        res += `\t${key}${allFields[key]}\n`;
    }
    res += `}\n`; // add the closing bracket for the entity
    return res;
}}
