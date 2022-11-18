// Imports
const typeDetect = require('type-detect');
// import typeDetect from 'type-detect';
const moment = require('moment');
// import moment from 'moment';

// This function is used to detect the type of content that is provided in a field by the user.
// We can and should improve this (TODO: 12-08-2022)
module.exports = { detectType: function(data, key) {
    let type = typeDetect(data);
    if (data === '') type = 'skip';
    else if (type === 'Object' || type === 'Array') return 'string';
    else if (key.toLowerCase().includes('name')) return 'string';
    else if (key.toLowerCase().includes('summary')) return 'string';
    else if (key.toLowerCase().includes('reference')) return 'string';
    else if (key.toLowerCase().endswith('date')) return 'date';
    else if (key.toLowerCase().endswith('dates')) return 'date';
    const bool = detectTypeBoolean(data, key, type);
    if (bool === 'boolean') return bool;
    const dec = detectTypeDecimal(data, key, type);
    if (dec === 'number') return dec;
    const date = detectTypeDate(data, key, type);
    if (date === 'date') {
        return date;
    } else if (type === 'date') {
        type = 'string'
    }
    return type;
}}

function detectTypeBoolean(data, key, type) {
    if (
        type === 'string' &&
        (
            ['true', 'false'].includes(data.toLowerCase())
            || (key.toLowerCase().startsWith('is') && (data === '0' || data === '1'))
        )
    ) type = 'boolean';
    return type;
}

function detectTypeDecimal(data, type) {
    if (type === 'string' && !data.includes(' ') && !isNaN(data)) 
        type = 'number';
    return type;
}

function detectTypeDate(data, type) {
    // only detect dates if they are at least 8 characters, yy/mm/dd
    if (data.length > 7 && data.length < 20 && (moment(data, 'MM/DD/YY hh:mm:ss A').isValid() || moment(data, moment.ISO_8601).isValid()))
        // this date check could be expanded with a user provided date format
        type = 'date';
    return type;
}
