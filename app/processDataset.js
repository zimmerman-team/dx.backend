// Imports
const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json');
const csvtojson = require('csvtojson');
const XLSX = require('xlsx');
// Project
const createModel = require('./apiBuilder.js').createModel;
// Constants
const DEFAULT_ENCODING = 'utf8';
const MODELFILE = path.join(__dirname, '../staging/db/schema.cds');

/**
 * We currently support the datasets of types:
 * xml, json, csv, xlsx (converted to csv)
 * 
 * We determine the filetype using the extension of the source file.
 */
module.exports = {processDataset: async function(filepath, name, source) {
    if (source.includes('.xml')) processXML(filepath, name);
    if (source.includes('.json')) processJSON(filepath, name);
    if (source.includes('.csv')) await processCSV(filepath, name);
    if (source.includes('.xlsx')) await processXLSX(filepath, name);
}}

function processXML(filepath, name) {
    console.debug("XML::Processing XML file: " + filepath);
    let data = JSON.parse(xml2json.toJson(fs.readFileSync(filepath, DEFAULT_ENCODING)));
    let appendString = "";
    try {
        const key = Object.keys(data)[0];
        const subkey = Object.keys(data[key])[0];
        appendString = createModel(data[key][subkey], name);
        
        // save the json converted content.
        const filepathJSON = filepath.replace('.xml', '.json');
        fs.writeFileSync(filepathJSON, JSON.stringify(data[key][subkey]));
        fs.unlinkSync(filepath);
    } catch (e) {
        console.log("CATCH::", e)
        appendString = createModel(data, name);
    }
    updateModelFile(appendString)
}

function processJSON(filepath, name) {
    console.debug("JSON::Processing JSON file: " + filepath);
    let data = JSON.parse(fs.readFileSync(filepath, DEFAULT_ENCODING));
    const appendString = createModel(data, name);
    updateModelFile(appendString)
}

async function processCSV(filepath, name) {
    console.debug("CSV::Processing CSV file: ", filepath);
    // TODO: re-implement HXL. If HXL is in the filename, the second row can contain HXL tags, these need to be removed
    let csvContent = fs.readFileSync(filepath, DEFAULT_ENCODING);
    csvContent = csvContent.split('\n'); // now an array of data rows
    // Remove spaces from the headers and remove any fields named ID.
    let headers = csvContent
        .shift()
        .replace(',ID', ',datasource_id')
        .replace(',iD', ',datasource_id')
        .replace(',Id', ',datasource_id')
        .replace(',id', ',datasource_id')
        .replace(' ', '');
    // write the updated file
    if (!fs.readFileSync(filepath, 'utf8').includes(csvContent)) {
        csvContent = [headers, ...csvContent].join('\n');
        fs.writeFileSync(filepath, csvContent);
    }

    await _modelForCSSV(filepath, name)
}

async function processXLSX(filepath, name) {
    console.debug("XLSX::Processing XLSX file: ", filepath)
    const filepathCSV = filepath.replace('.xlsx', '.csv');
    // if the file is already converted, remove the xlsx file and continue
    // convert the xlsx file to a csv file
    let workbook = XLSX.readFile(filepath);
    XLSX.writeFile(workbook, filepathCSV, { bookType: 'csv' });
    let csvContent = fs.readFileSync(filepathCSV, DEFAULT_ENCODING);
    csvContent = csvContent.split('\n'); // now an array of data rows
    // clean the headers row and update the file
    let headers = csvContent.shift().replaceAll(/[^a-z0-9,]/gi, '').replace(' ', '');
    csvContent = [headers, ...csvContent].join('\n');
    fs.writeFileSync(filepathCSV, csvContent);
    fs.unlinkSync(filepath);

    await _modelForCSSV(filepathCSV, name)
}

async function _modelForCSSV(filepath, name) {
    // Get the data into JSON format to pre-process for the OData model
    const json_data = await csvtojson().fromFile(filepath)
    let appendString = createModel(json_data, name);
    updateModelFile(appendString)
}

function updateModelFile(content) {
    // write the new model to the model file.
    if (!fs.readFileSync(MODELFILE, 'utf8').includes(content)) {
        fs.appendFileSync(MODELFILE, content);
    }
}