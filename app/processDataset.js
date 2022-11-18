// Imports
// import path from 'path';
// import xml2json from 'xml2json';
// import csvtojson from 'csvtojson';
// import XLSX from 'xlsx';
// import fs from 'fs';

const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json');
const csvtojson = require('csvtojson');
const XLSX = require('xlsx');

// import { fileURLToPath } from 'url';
// Project
// import { createModelFile } from './apiBuilder.js';
const createModelFile = require('./apiBuilder.js').createModelFile;
// Constants
const DEFAULT_ENCODING = 'utf8';
// const __dirname = path.dirname(fileURLToPath(import.meta.url));
const modelFile = path.join(__dirname, '../staging/db/schema.cds');

module.exports = {processDataset: async function(filepath, name, appendString, source) {
    if (source.includes('.xml')) appendString = processXML(filepath, name, appendString);
    if (source.includes('.json')) appendString = processJSON(filepath, name, appendString);
    if (source.includes('.csv')) await processCSV(filepath, name);
    if (source.includes('.xlsx')) processXLSX(filepath);
    return appendString;
}}

function processXML(filepath, name, appendString) {
    // process XML files
    let data = xml2json.toJson(fs.readFileSync(filepath, DEFAULT_ENCODING));
    appendString += createModelFile(data, name);
    return appendString;
}

function processJSON(filepath, name, appendString) {
    // process XML files
    let data = JSON.parse(fs.readFileSync(filepath, DEFAULT_ENCODING));
    appendString += createModelFile(data, name);
    return appendString;
}

async function processCSV(filepath, name) {
    // process CSV files
    // If HXL is in the filename, the second row can contain HXL tags, these need to be removed
    if (name.includes('HXL')) {
        let csvContent = fs.readFileSync(filepath, DEFAULT_ENCODING);
        csvContent = csvContent.split('\n'); // now an array of data rows
        let headers = csvContent.shift().replace(',id', ',datasource_id').replace(' ', '');
        let hxlTags = csvContent.shift();
        if (hxlTags.match(/#.*,#.*,#.*/g)?.length > 0) {
            // write the updated file
            if (!fs.readFileSync(filepath, 'utf8').includes(csvContent)) {
                csvContent = [headers, ...csvContent].join('\n');
                console.debug('RELOAD - HXL datasource updated.');
                fs.writeFileSync(filepath, csvContent);
            }
        }
    }

    // Get the data into JSON format to pre-process for the OData model
    res = await csvtojson().fromFile(filepath).then(json => {
        // This method is Async, and therefore requires a direct write into the model file,
        // if we write to the model file using the appendString +=, this will be accessed
        // before the appendString is loaded with the data.
        let appendString = createModelFile(json, name);
        if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
            console.debug('RELOAD - ModelFile updated.');
            fs.appendFileSync(modelFile, appendString);
        }
    });
}

function processXLSX(filepath) {
    // convert XLSX files to csv
    const filepathCSV = filepath.replace('.xlsx', '.csv');

    // if the file is already converted, remove the xlsx file and continue
    if (fs.existsSync(filepathCSV)) {
        let csvContent = fs.readFileSync(filepathCSV, DEFAULT_ENCODING);
        csvContent = csvContent.split('\n'); // now an array of data rows
        // clean the headers row and update the file
        let headers = csvContent.shift().replaceAll(/[^a-z0-9,]/gi, '');
        csvContent = [headers, ...csvContent].join('\n');
        console.debug('RELOAD - XLSX datasource converted.');
        fs.writeFileSync(filepathCSV, csvContent);
        fs.unlinkSync(filepath);
    } else {
        // convert the xlsx file to a csv file
        let workbook = XLSX.readFile(filepath);
        XLSX.writeFile(workbook, filepathCSV, { bookType: 'csv' });
    }
}
