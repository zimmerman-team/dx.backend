// Imports
import typeDetect from 'type-detect';
import moment from 'moment';

// This function is used to detect the type of content that is provided in a field by the user.
// We can and should improve this (TODO: 12-08-2022)
export function detectType(data, key) {
    let type = typeDetect(data);
    if (data === '') type = 'skip';
    else if (type === 'Object' || type === 'Array') return 'string';
    else if (key.toLowerCase().includes('name')) return 'string';
    else if (key.toLowerCase().includes('summary')) return 'string';
    else if (key.toLowerCase().includes('reference')) return 'string';
    else if (key.toLowerCase().includes('date')) return 'date';

    type = detectTypeBoolean(data, key, type);
    type = detectTypeDecimal(data, type);
    type = detectTypeDate(data, type);
    return type;
}

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
    if (data.length > 7 && (moment(data, 'MM/DD/YY hh:mm:ss A').isValid() || moment(data, moment.ISO_8601).isValid()))
        // this date check could be expanded with a user provided date format
        type = 'date';
    return type;
}
