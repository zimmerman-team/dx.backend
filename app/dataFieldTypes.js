// Project
const inferTypes = require("@rawgraphs/rawgraphs-core").inferTypes;
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

module.exports = {getFieldTypes: function(data) {
    // DATA is a json object.
    let allFields = {};
    const inferredTypes = inferTypes(data);        
    for (let item in inferredTypes) {
        allFields[item] = inferredTypeToType(inferredTypes[item]);
    };
    return allFields;
}}

const inferredTypeToType = (itemType) => {
    // determine and return our SAP cloud model variant of the field type.
    try {
        if (typeof itemType === 'object') {
            return lookupType[itemType['type']];
        }
        const ret = lookupType[itemType];
        return ret ? ret : lookupType['string'];
    } catch(_) {
        // default return a string, do not let this break the process.
        return lookupType['string'];
    }
}
