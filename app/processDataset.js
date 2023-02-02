// Imports
const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json');
const csvtojson = require('csvtojson');
const XLSX = require('xlsx');
const csvString = require('csv-string');

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
    /**
     * If we receive XML, we want to convert this to CSV. However, searching through npmjs
     * there are no packages directly converting XML to CSV. So we try converting to JSON first.
     * xml2js has 16m weekly downloads. xml2json has 100k weekly downloads.
     * The current approach is: reading the XML file, converting it to JSON.
     * We then take the key
    */
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
    const separator = csvString.detect(csvContent[0]);  // find the separator used in the csv file
    let headers = _cleanCSVHeaders(csvContent, separator);
    csvContent = [headers, ...csvContent].join('\n'); // recombine the headers and content
    csvContent = _cleanCSVLinebreaks(headers.split(',').length, csvContent, separator);
    fs.writeFileSync(filepath, csvContent);
    await _modelForCSSV(filepath, name, separator)
}

async function processXLSX(filepath, name) {
    console.debug("XLSX::Processing XLSX file: ", filepath)
    const filepathCSV = filepath.replace('.xlsx', '.csv');
    // if the file is already converted, remove the xlsx file and continue
    // convert the xlsx file to a csv file
    let workbook = XLSX.readFile(filepath);
    XLSX.writeFile(workbook, filepathCSV, { bookType: 'csv' });
    await processCSV(filepathCSV, name);
    fs.unlinkSync(filepath);
}

async function _modelForCSSV(filepath, name, separator) {
    // Get the data into JSON format to pre-process for the OData model
    const json_data = await csvtojson({delimiter: separator}).fromFile(filepath)
    let appendString = createModel(json_data, name);
    updateModelFile(appendString)
}

function _cleanCSVHeaders(csvContent, separator) {
    // ensure there is no duplicate 'id' field and replace spaces
    let headers = csvContent
        .shift()
        .replace(',ID', ',datasource_id')
        .replace(',iD', ',datasource_id')
        .replace(',Id', ',datasource_id')
        .replace(',id', ',datasource_id')
        .replace(' ', '')
        .replace(/\s/g, '')
    if (separator === ',') {
        headers = headers
            // split on commas but not on commas between quotes
            .match(/(".*?"|[^",\s]+)(?=\s*,|\s*$)/g) 
            // replace non-alphanumeric characters with nothing and prefix pure numbers with n
            .map((h) =>  h.replaceAll(/[^a-z0-9]/gi, '').replaceAll(/^\d+/g, 'n$&')) 
            // truncate to 124 characters
            .map((h) =>  h.length > 124 ? h.substring(0, 124) : h)
            // recombine the headers
            .join(',');
    } else {
        // same as above without exception for commas between quotes
        headers = headers
            .split(separator)
            .map((h) =>  h.replaceAll(/[^a-z0-9]/gi, '').replaceAll(/^\d+/g, 'n$&'))
            .map((h) =>  h.length > 124 ? h.substring(0, 124) : h)
            .join(separator);
    }
    return headers
}

function _cleanCSVLinebreaks (cols, csv, separator) {
    // csv is the string containing the csv
    // cols is the number of columns in the csv
    // replace all linebreaks with a space unless we have seen cols-1 number of commas
    let clean = '';
    let commas = 0;
    let ignoreComma = false;
    for (let i = 0; i < csv.length; i++) {
        if (csv[i] === '"') ignoreComma = !ignoreComma; // ignore quoted values
        if (csv[i] === separator && !ignoreComma) commas++; // increment the comma counter
        // if there is a linebreak and we have not seen cols-1 commas, then replace the linebreak with a space
        if ((csv[i] === '\n' || csv[i] === '\r') && commas < cols-1) {
            clean += ' ';
        }
        else {
            // otherwise, add the character to the clean string unless it is a linebreak, ensure it is a '\n' not a '\r'
            clean += (csv[i] === '\n' || csv[i] === '\r') ? '\n' : csv[i];
        }
        if (commas === cols-1 && (csv[i] === '\n' || csv[i] === '\r')) {
            commas = 0;
        }
    }
    return clean;
}

function updateModelFile(content) {
    // write the new model to the model file.
    if (!fs.readFileSync(MODELFILE, 'utf8').includes(content)) {
        fs.appendFileSync(MODELFILE, content);
    }
}
