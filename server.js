const cds = require('@sap/cds')
const path = require('path')
const fs = require('fs')
const xml2json = require('xml2json')

cds.once('bootstrap', () => {
    const sources = [
        'IATIActivity.json',
        'IATIBudget.json',
        'IATITransaction.json',
        'TGFAllocation.json',
        'IATIAllBudgets.json',
    ]
    const lookupType = {
        // more types can be found at https://docs.progress.com/bundle/datadirect-hybrid-data-pipeline-46/page/Entity-Data-Model-EDM-types-for-OData-Version-4.html
        'number': '  : Decimal(32,2);',
        'string': '  : localized String(1111);',
        'object': '  : localized String(1111);',
    }

    console.log("Preparing data models...")
    for (let i in sources) {
        const fs = require('fs')
        let data = []
        if (sources[i].includes('xml')) {
            data = xml2json.toJson(fs.readFileSync(`/home/zimmerman/Projects/OData/SAP/data/db/data/data-${sources[i]}`, 'utf8'))
        } else if (sources[i].includes('.json')) {
            data = JSON.parse(fs.readFileSync(`/home/zimmerman/Projects/OData/SAP/data/db/data/data-${sources[i]}`, 'utf8'))
        }

        const name = path.parse(sources[i]).name;

        let allFields = {}
        data.forEach((item) => {
            Object.keys(item).forEach((key) => {
                let keyType = typeof item[key]
                // if (keyType === 'object') keyType = typeof item[key][0]
                allFields[key] = keyType
            })
        })

        let appendString = `\nentity ${name} : managed {\n\tkey ID : Integer;\n`

        // add the keys and their EDM type to the 
        for (let key in allFields) {
            appendString += `\t${key}${lookupType[allFields[key]]}\n`
            // modelType[key] = { type: lookupType[allFields[key]] }
        }
        appendString += `}\n`

        const modelFile = '/home/zimmerman/Projects/OData/SAP/data/db/schema.cds'
        // check if the name already exists in /home/zimmerman/Projects/OData/SAP/data/db/schema.cds
        if (!fs.readFileSync(modelFile, 'utf8').includes(name)) {
            fs.appendFileSync(modelFile, appendString)
        }
        // fs.appendFileSync('./model.js', MODELFILE_CLOSE)
    }

    console.log("Preparing data services...")
    let writeStr = `using { data as my } from '../db/schema';\nservice CatalogService @(path:'/data') {\n`
    for (let i in sources) {
        const name = path.parse(sources[i]).name;
        writeStr += `\t@readonly entity ${name} as SELECT from my.${name} {*} excluding { createdBy, modifiedBy };\n`
    }
    writeStr += `}\n`

    const serviceFile = '/home/zimmerman/Projects/OData/SAP/data/srv/data-service.cds'
    if (!fs.readFileSync(serviceFile, 'utf8').includes(writeStr)) {
        fs.writeFileSync(serviceFile, writeStr)
    }

    console.log("Ready to serve your data at http://localhost:4004/")
})
