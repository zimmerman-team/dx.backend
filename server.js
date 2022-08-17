import 'dotenv/config';
import cds from '@sap/cds'
import path from 'path'
import fs from 'fs'
import xml2json from 'xml2json'
import csvtojson from 'csvtojson'
import typeDetect from 'type-detect'
import moment from 'moment'
import XLSX from 'xlsx'
import { fileURLToPath } from 'url'
import _ from 'lodash'

const __dirname = path.dirname(fileURLToPath(import.meta.url));
console.log("__dirname: " + __dirname)

// CONSTS
const serviceFile = path.join(__dirname, '/srv/data-service.cds')
const modelFile = path.join(__dirname, '/db/schema.cds')
const dataFolder = path.join(__dirname, '/db/data')
const lookupType = {
    // more types can be found at
    // https://cap.cloud.sap/docs/cds/types
    'number': '  : Decimal;',
    'string': '  : String;',
    'Object': '  : String;',
    'Array': '  : String;',
    'undefined': '  : String;',
    'date': '  : DateTime;',
    'boolean': '  : Boolean;',
}
const DEFAULT_ENCODING = 'utf8'

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
        const name = path.parse(sources[i]).name.replace('data-', '')

        // Generate empty configs for the data source if they don't exist
        generateConfigs(name)
        
        // if the name is already in the data-service file, skip.
        if (fs.readFileSync(modelFile, 'utf8').includes(name)) continue

        console.debug('-- Preparing data model for ' + sources[i])
        appendString = processDataset(filepath, name, appendString, sources[i])
    }

    // write the new model to the model file.
    if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
        fs.appendFileSync(modelFile, appendString)
    }
    createServiceFile() // make sure the service file is up to date with the latest models.
    console.debug("Ready to serve your data at http://localhost:4004/")
})

function processDataset(filepath, name, appendString, source) {
    if (source.includes('.xml')) appendString = processXML(filepath, name, appendString)
    if (source.includes('.json')) appendString = processJSON(filepath, name, appendString)
    if (source.includes('.csv')) processCSV(filepath, name)
    if (source.includes('.xlsx')) processXLSX(filepath)
    return appendString
}

function processXML(filepath, name, appendString) {
    // process XML files
    let data = xml2json.toJson(fs.readFileSync(filepath, DEFAULT_ENCODING))
    appendString += createModelFile(data, name)
    return appendString
}

function processJSON(filepath, name, appendString) {
    // process XML files
    let data = JSON.parse(fs.readFileSync(filepath, DEFAULT_ENCODING))
    appendString += createModelFile(data, name)
    return appendString
}

function processCSV(filepath, name) {
    // process CSV files
    // If HXL is in the filename, the second row can contain HXL tags, these need to be removed
    if (name.includes('HXL')) {
        let csvContent = fs.readFileSync(filepath, DEFAULT_ENCODING)
        csvContent = csvContent.split('\n') // now an array of data rows
        let headers = csvContent.shift().replace(',id', ',datasource_id').replace(' ', '')
        let hxlTags = csvContent.shift()
        if (hxlTags.match(/#.*,#.*,#.*/g)?.length > 0) {
            // write the updated file
            if (!fs.readFileSync(filepath, 'utf8').includes(csvContent)) {
                csvContent = [headers, ...csvContent].join('\n')
                fs.writeFileSync(filepath, csvContent)
            }
        } 
    }

    // Get the data into JSON format to pre-process for the OData model
    csvtojson().fromFile(filepath).then(json => {
        // This method is Async, and therefore requires a direct write into the model file,
        // if we write to the model file using the appendString +=, this will be accessed
        // before the appendString is loaded with the data.
        let appendString = createModelFile(json, name)
        if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
            fs.appendFileSync(modelFile, appendString)
        }
    })
    
}

function processXLSX(filepath) {
    // convert XLSX files to csv
    const filepathCSV = filepath.replace('.xlsx', '.csv')

    // if the file is already converted, remove the xlsx file and continue
    if (fs.existsSync(filepathCSV)) {
        let csvContent = fs.readFileSync(filepathCSV, DEFAULT_ENCODING)
        csvContent = csvContent.split('\n') // now an array of data rows
        // clean the headers row and update the file
        let headers = csvContent.shift().replace(',id', ',datasource_id').replaceAll(' ', '')
        csvContent = [headers, ...csvContent].join('\n')
        fs.writeFileSync(filepathCSV, csvContent)
        fs.unlinkSync(filepath)
    } else {
        // convert the xlsx file to a csv file
        let workbook = XLSX.readFile(filepath)
        XLSX.writeFile(workbook, filepathCSV, { bookType: 'csv' })
    }
}


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
    const allFields = getMostCommonFieldTypes(data)
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
        res += `\t${key}${allFields[key]}\n`
    }
    res += `}\n` // add the closing bracket for the entity
    return res 
}

function getMostCommonFieldTypes(data) {
    let allFields = {}
    // gather all the headers and the type of their content
    data.forEach((item) => {
        Object.keys(item).forEach((key) => {
            if (!(Object.keys(allFields).includes(key))) allFields[key] = []
            const keyType = detectType(item[key], key)
            keyType !== 'skip' && allFields[key].push(keyType)
        })
    })

    const mostOf = (fields) => {
        // This approach is O(n).
        if(fields.length == 0) return 'string'
        let mostOfMapping = {}
        let maxEl = fields[0], maxCount = 1

        for(const element of fields) {
            let el = element
            if(mostOfMapping[el] == null) mostOfMapping[el] = 1
            else mostOfMapping[el]++
            if(mostOfMapping[el] > maxCount){
                maxEl = el
                maxCount = mostOfMapping[el]
            }
        }
        return maxEl
    }
    Object.keys(allFields).forEach((key) => {
        // replace the array at the key with the most common type
        // compared to the lookup object keys.
        allFields[key] = lookupType[mostOf(allFields[key])]
    })
    return allFields
}

// This function is used to detect the type of content that is provided in a field by the user.
// We can and should improve this (TODO: 12-08-2022)
function detectType(data, key) {
    let type = typeDetect(data)
    if (data === '') type = 'skip'
    else if (type === 'Object' || type === 'Array') return 'string'
    else if (key.toLowerCase().includes('name')) return 'string'
    else if (key.toLowerCase().includes('summary')) return 'string'
    else if (key.toLowerCase().includes('reference')) return 'string'
    else if (key.toLowerCase().includes('date')) return 'date'

    type = detectTypeBoolean(data, key, type)
    type = detectTypeDecimal(data, type)
    type = detectTypeDate(data, type)
    return type
}

function detectTypeBoolean(data, key, type) {
    if (
        type === 'string' &&
        (
            ['true', 'false'].includes(data.toLowerCase())
            || (key.toLowerCase().startsWith('is') && (data === '0' || data === '1'))
        )
    ) type = 'boolean'
    return type
}

function detectTypeDecimal(data, type) {
    if (type === 'string' && !data.includes(' ') && !isNaN(data)) type = 'number'
    return type
}

function detectTypeDate(data, type) {
    // only detect dates if they are at least 8 characters, yy/mm/dd
    if (data.length > 7 && (moment(data, 'MM/DD/YY hh:mm:ss A').isValid() || moment(data, moment.ISO_8601).isValid())) {
        // this date check could be expanded with a user provided date format
        type = 'date'
    }
    return type
}

// This function synchronously reads in all of the available data mapping filepaths from the data explorer
function listConfigFiles(dir) {
    fs.readdirSync(dir).forEach(file => {
        const abs = path.join(dir, file)
        if (fs.statSync(abs).isDirectory()) return listConfigFiles(abs)
        else if (path.extname(abs) === '.json') return configPaths.push(abs)
    });
}

// Function to generate a configuration for a given dataset name.
// Currently, this duplicates the initial configuration within the file and appends a new key to each configuration,
// where the value will be used for that new dataset.
function generateConfigs(name) {
    // Generate empty json config objects for the data source in the data explorer project folder
    configPaths.forEach((configPath) => {
        let config = JSON.parse(fs.readFileSync(configPath, 'utf8'))
        const configType = typeDetect(config)
        const originalConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'))
        // Specific files first
        config = generateDataSourceConfigs(name, configPath, config)
        config = generateDataSetConfigs(name, configPath, config)
        config = generateFilterDefaultConfigs(name, configPath, config)
        if (
            // only generate the configs if it was not a specific file and if the config does not yet exist
            _.isEqual(config, originalConfig) && (
                (configType === 'Object' && !Object.keys(config).includes(name)) ||
                (configType === 'Array' && !config.includes(name))
            )
        ) {
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
        }
        // write the config to the data source config file only if it has changed
        if (!_.isEqual(config, originalConfig)) {
            fs.writeFileSync(configPath, JSON.stringify(config, null, 2))
        }
    })
}

function generateDataSourceConfigs(name, configPath, config) {
    if (configPath.includes('mapping/datasources.json')) {
        if (!config.includes(name)) config.push(name)
    }
    return config
}

function generateDataSetConfigs(name, configPath, config) {
    if (configPath.includes('mapping/datasets.json')) {
        if (!Object.keys(config).includes(name)) {
            config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]))
            // for each key, set the value to false
            for (let key in config[name]) {
                config[name][key] = false
            }
        }
    }
    return config
}

function generateFilterDefaultConfigs(name, configPath, config) {
    if (configPath.includes('filtering/index.json')) {
        if (!Object.keys(config).includes(name)) {
            config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]))
        }
    }
    return config
}

// Function to remove string values from the created config, where obj is the config object with the key selected.
// We could introduce a "field name filter", for example allowing all OData api filtering and value names to remain in the config.
function clearConfig (obj) {
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
