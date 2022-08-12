const cds = require('@sap/cds')
const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json')
const csvtojson = require('csvtojson')
const typeDetect = require('type-detect')
const moment = require('moment')
const XLSX = require('xlsx')
require('dotenv').config()

// CONSTS
const serviceFile = path.join(__dirname, '/srv/data-service.cds')
const modelFile = path.join(__dirname, '/db/schema.cds')
const dataFolder = path.join(__dirname, '/db/data')
const lookupType = {
    // more types can be found at
    // https://cap.cloud.sap/docs/cds/types
    'number': '  : Decimal;',
    'string': '  : String;',
    'object': '  : String;',
    'undefined': '  : String;',
    'array': '  : String;',
    'date': '  : DateTime;',
    'boolean': '  : Boolean;',
}

console.debug("Gathering config files...")
let configPaths = []
listConfigFiles(process.env.DATA_EXPLORER_SERVER)

cds.on('loaded', () => {
    // Gather the names of the datasources within the datasource folder
    let sources = []
    fs.readdirSync(dataFolder).forEach(file => { sources.push(file) })

    // create an empty string which will contain the models to be appended to the model file
    console.debug("Preparing data models...")
    let appendString = ``
    for (let i in sources) {
        const filepath = path.join(__dirname, `/db/data/${sources[i]}`)
        // if the file is a directory, skip
        if (fs.statSync(filepath).isDirectory()) continue
        const extension = 'utf8'
        const name = path.parse(sources[i]).name.replace('data-', '')

        // if the name is already in the data-service file, skip.
        if (fs.readFileSync(modelFile, 'utf8').includes(name)) continue

        // Generate empty configs for the data source if they don't exist
        generateConfigs(name);
        
        console.debug('-- Preparing data model for ' + sources[i])
        if (sources[i].includes('xml')) {
            // process XML files
            let data = xml2json.toJson(fs.readFileSync(filepath, extension))
            appendString += createModelFile(data, name)
        } else if (sources[i].includes('.json')) {
            // process JSON files
            let data = JSON.parse(fs.readFileSync(filepath, extension))
            appendString += createModelFile(data, name)
        } else if (sources[i].includes('.xlsx')) {
            // convert XLSX files to csv
            const filepathCSV = filepath.replace('.xlsx', '.csv')

            // if the file is already converted, remove the xlsx file and continue
            if (fs.existsSync(filepathCSV)) {
                let csvContent = fs.readFileSync(filepathCSV, extension)
                csvContent = csvContent.split('\n') // now an array of data rows
                // clean the headers row and update the file
                let headers = csvContent.shift().replace(',id', ',datasource_id').replaceAll(' ', '')
                csvContent = [headers, ...csvContent].join('\n')
                fs.writeFileSync(filepathCSV, csvContent)
                fs.unlinkSync(filepath)
                continue
            } else {
                // convert the xlsx file to a csv file
                let workbook = XLSX.readFile(filepath)
                XLSX.writeFile(workbook, filepathCSV, { bookType: 'csv' })
            }
        } else if (sources[i].includes('.csv')) {
            // process CSV files
            csvContent = fs.readFileSync(filepath, extension)
            csvContent = csvContent.split('\n') // now an array of data rows
            headers = csvContent.shift().replace(',id', ',datasource_id').replace(' ', '')
            // If HXL is in the filename, the second row can contain HXL tags, these need to be removed
            if (name.includes('HXL')) {
                hxlTags = csvContent.shift()
                if (hxlTags.match(/#.*,#.*,#.*/g)?.length > 0) {
                    // write the updated file
                    if (!fs.readFileSync(filepath, 'utf8').includes(csvContent)) {
                        csvContent = [headers, ...csvContent].join('\n')
                        fs.writeFileSync(filepath, csvContent)
                    }
                } else {
                    csvContent = [headers, hxlTags, ...csvContent].join('\n')
                }
            }

            // Get the data into JSON format to pre-process for the OData model
            csvtojson().fromFile(filepath).then(json => {
                // This method is Async, and therefore requires a direct write into the model file,
                // if we write to the model file using the appendString +=, this will be accessed
                // before the appendString is loaded with the data.
                appendString = createModelFile(json, name)
                if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
                    fs.appendFileSync(modelFile, appendString)
                }
            })
        }
    }

    // write the new model to the model file.
    if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
        fs.appendFileSync(modelFile, appendString)
    }
    createServiceFile() // make sure the service file is up to date with the latest models.
    console.debug("Ready to serve your data at http://localhost:4004/")
})

// Create a service file for each of the models available in the data/schema.cds folder.
function createServiceFile() {
    console.debug("Preparing data services...")
    let writeStr = `using { data as my } from '../db/schema';\nservice CatalogService @(path:'/data') {\n`

    // get the name from each created model, found in the model file
    let modelFileContent = fs.readFileSync(modelFile, 'utf8')
    let modelIndexes = modelFileContent.match(/entity ([a-zA-Z0-9]+) : managed/g)
    for (let i in modelIndexes) {
        // remove surrounding cds tags from model name and create a service string
        const name = modelIndexes[i].replace('entity ', '').replace(' : managed', '')
        writeStr += `\t@readonly entity ${name} as SELECT from my.${name} {*} excluding { createdAt, createdBy, modifiedAt, modifiedBy };\n`
    }
    writeStr += `}\n` // close the new service file string
    if (!fs.readFileSync(serviceFile, 'utf8').includes(writeStr)) {
        fs.writeFileSync(serviceFile, writeStr)
    }
}

// Create a model for the data source
function createModelFile(data, name) {
    let allFields = {}
    // gather all the headers and the type of their content
    data.forEach((item) => {
        Object.keys(item).forEach((key) => {
            let keyType = detectType(item[key], allFields, key)
            allFields[key] = keyType
        })
    })

    // create the entity header
    let res = `\nentity ${name} : managed {\n\t`
    // check if an ID is present within the data source, if not, add a default ID field
    let idFound = false
    for (let key in allFields) {
        if (key == 'id') {
            idFound = true
            break
        }
    }
    if (!idFound) res += `key ID : Integer;\n`

    // add each key with its type to the entity
    for (let key in allFields) {
        let lookup = lookupType[allFields[key]] || lookupType['undefined']
        res += `\t${key}${lookup}\n`
    }
    return res += `}\n` // add the closing bracket for the entity
}

// This function is used to detect the type of content that is provided in a field by the user.
// We can and should improve this (TODO: 12-08-2022)
function detectType(data, allFields, key) {
    if (allFields[key]) return allFields[key]
    if (data === '') {
        return key in allFields ? allFields[key] : 'undefined'
    }

    if (typeof data === 'string' && data.toLowerCase().includes('name')) return 'string'
    if (typeof data === 'string' && data.toLowerCase().includes('summary')) return 'string'
    if (typeof data === 'string' && data.toLowerCase().includes('reference')) return 'string'
    if (typeof data === 'string' && data.toLowerCase().includes('date')) return 'date'

    let type = typeDetect(data)
    if (type === 'string') {
        if (data.toLowerCase() in ['true', 'false']) type = 'boolean'
        // if the key starts with the string 'Is'
        if (key.toLowerCase().startsWith('is') && (data === '0' || data === '1')) type = 'boolean'
        if (data.toLowerCase() === 'true' || data.toLowerCase() === 'false') type = 'boolean'
        if (!data.includes(' ') && !isNaN(data)) type = 'number'
        // else if (moment(data, moment.RFC_2822, true).isValid()) type = 'date'
        // else if (moment(data, moment.ISO_8601, true).isValid()) type = 'date'
        // only detect dates if they are at least 8 characters, yy/mm/dd
        else if (data.length > 7) {
            // this date check could be expanded with a user provided date format
            if (moment(data, 'MM/DD/YY hh:mm:ss A').isValid()) type = 'date'
            if (moment(data, moment.ISO_8601).isValid()) type = 'date'
        }
    }

    // todo: check https://github.com/cigolpl/type-detection/blob/master/index.js if we can introduce better type detection
    return type
}

// This function synchronously reads in all of the available data mapping filepaths from the data explorer
function listConfigFiles(dir) {
    fs.readdirSync(dir).forEach(file => {
        const abs = path.join(dir, file);
        if (fs.statSync(abs).isDirectory()) return listConfigFiles(abs);
        else if (path.extname(abs) === '.json') return configPaths.push(abs);
    });
}

// Function to generate a configuration for a given dataset name.
// Currently, this duplicates the initial configuration within the file and appends a new key to each configuration,
// where the value will be used for that new dataset.
function generateConfigs(name) {
    // Generate empty json config objects for the data source in the data explorer project folder
    configPaths.forEach(configPath => {
        const config = JSON.parse(fs.readFileSync(configPath, 'utf8'))
        if (!Object.keys(config).includes(name)) {
            if (Array.isArray(config[Object.keys(config)[0]])) {
                // if the key contains an array, check each element if they are an object
                // copy with deep nested objects using JSON stringify and parse.
                config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]))
                config[Object.keys(config)[1]].forEach(item => {
                    if (typeof item === 'object') clearConfig(item)
                })
            } else {
                config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]))
                clearConfig(config[name])
            }
            // write the config to the data source config file
            fs.writeFileSync(configPath, JSON.stringify(config, null, 2))
        }
    })
}

// Function to remove string values from the created config, where obj is the config object with the key selected.
// We could introduce a "field name filter", for example allowing all OData api filtering and value names to remain in the config.
const clearConfig = (obj) => {
    Object.keys(obj).forEach(key => {
        // if the data is an array, check each element if they are an object.
        if (Array.isArray(obj[key])) {
            obj[key].forEach(item => {
                if (typeof item === 'object') clearConfig(item)
            })
        // if the element is an object, process the object's keys.
        } else if (typeof obj[key] === 'object') {
            clearConfig(obj[key])
        // if the element is a string, remove the string value.
        } else {
            obj[key] = ''
        }
    })
}