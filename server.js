const cds = require('@sap/cds')
const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json')
const csvtojson = require('csvtojson')

const serviceFile = '/home/zimmerman/Projects/OData/SAP/data/srv/data-service.cds'
const modelFile = '/home/zimmerman/Projects/OData/SAP/data/db/schema.cds'
const dataFolder = `/home/zimmerman/Projects/OData/SAP/data/db/data`

const lookupType = {
    // more types can be found at https://docs.progress.com/bundle/datadirect-hybrid-data-pipeline-46/page/Entity-Data-Model-EDM-types-for-OData-Version-4.html
    'number': '  : Decimal(32,2);',
    'string': '  : localized String(1111);',
    'object': '  : localized String(1111);',
}

cds.on('loaded', () => {
    // Gather the names of the datasources within the datasource folder
    let sources = []
    fs.readdirSync(dataFolder).forEach(file => {sources.push(file)})

    // create an empty string which will contain the models to be appended to the model file
    let appendString = ``

    console.log("Preparing data models...")
    for (let i in sources) {
        const filepath = `/home/zimmerman/Projects/OData/SAP/data/db/data/${sources[i]}`
        const extension = 'utf8'
        const name = path.parse(sources[i]).name.replace('data-', '')

        // if the name is already in the data-service file, skip.
        if (fs.readFileSync(modelFile, 'utf8').includes(name)) continue
        
        if (sources[i].includes('xml')) {
            // process XML files
            let data = xml2json.toJson(fs.readFileSync(filepath, extension))
            appendString += createModelFile(data, name)
        } else if (sources[i].includes('.json')) {
            // process JSON files
            let data = JSON.parse(fs.readFileSync(filepath, extension))
            appendString += createModelFile(data, name)
        } else if (sources[i].includes('.csv')) {
            // process CSV files
            csvContent = fs.readFileSync(filepath, extension)
            csvContent = csvContent.split('\n') // now an array of data rows
            headers = csvContent.shift().replace(',id', ',datasource_id')
            // If HXL is in the filename, the second row can contain HXL tags, these need to be removed
            if (name.includes('HXL')) {
                hxlTags = csvContent.shift()
                if (hxlTags.includes('#')) {
                // write the updated file
                    if (!fs.readFileSync(filepath, 'utf8').includes(csvContent)) {
                        console.log("UPDATE THE HXL CSV FILE!!!")
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

    // check if the name already exists in /home/zimmerman/Projects/OData/SAP/data/db/schema.cds
    if (!fs.readFileSync(modelFile, 'utf8').includes(appendString)) {
        fs.appendFileSync(modelFile, appendString)
    }
    createServiceFile()
    console.log("Ready to serve your data at http://localhost:4004/")
})

function createServiceFile() {
    console.log("Preparing data services...")
    let writeStr = `using { data as my } from '../db/schema';\nservice CatalogService @(path:'/data') {\n`
    
    // get the name from each created model, found in the model file
    let modelFileContent = fs.readFileSync(modelFile, 'utf8')
    let modelIndexes = modelFileContent.match(/entity ([a-zA-Z0-9]+) : managed/g)
    for (let i in modelIndexes) {
        const name = modelIndexes[i].replace('entity ', '').replace(' : managed', '')
        writeStr += `\t@readonly entity ${name} as SELECT from my.${name} {*} excluding { createdBy, modifiedBy };\n`
    }
    writeStr += `}\n`
    if (!fs.readFileSync(serviceFile, 'utf8').includes(writeStr)) {
        fs.writeFileSync(serviceFile, writeStr)
    }
}

function createModelFile(data, name) {
    let allFields = {}
    // gather all the headers and the type of their content
    data.forEach((item) => {
        Object.keys(item).forEach((key) => {
            let keyType = typeof item[key]
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
        res += `\t${key}${lookupType[allFields[key]]}\n`
    }
    return res += `}\n` // add the closing bracket for the entity
}
