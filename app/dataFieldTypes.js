// Project
// import { detectType } from './typeDetection.js'
const detectType = require('./typeDetection').detectType;
// Constants
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
};

module.exports = {getMostCommonFieldTypes: function(data) {
    let allFields = {};
    // gather all the headers and the type of their content
    data.forEach((item) => {
        Object.keys(item).forEach((key) => {
            if (!(Object.keys(allFields).includes(key))) allFields[key] = [];
            const keyType = detectType(item[key], key);
            keyType !== 'skip' && allFields[key].push(keyType);
        })
    })

    Object.keys(allFields).forEach((key) => {
        // replace the array at the key with the most common type
        // compared to the lookup object keys.
        allFields[key] = lookupType[mostOf(allFields[key])];
    });
    return allFields;
}}

function mostOf(fields) {
    // This approach is O(n).
    if (fields.length == 0) return 'string';
    let mostOfMapping = {};
    let maxEl = fields[0], maxCount = 1;

    for (const element of fields) {
        let el = element;
        if (mostOfMapping[el] == null) mostOfMapping[el] = 1;
        else mostOfMapping[el]++;
        if (mostOfMapping[el] > maxCount) {
            maxEl = el;
            maxCount = mostOfMapping[el];
        }
    }
    return maxEl;
}