// Imports
import path from 'path';
import fs from 'fs';
import typeDetect from 'type-detect';
import _ from 'lodash';
// Constants
const configPaths = [];
listConfigFiles(process.env.DATA_EXPLORER_SERVER);

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
export function generateConfigs(name) {
    console.debug(`Checking DX config files for ${name}...`);
    // Generate empty json config objects for the data source in the data explorer project folder
    configPaths.length > 0 && configPaths.forEach((configPath) => {
        let config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        const configType = typeDetect(config);
        const originalConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
        // Specific files first
        // config = generateDataSourceConfigs(name, configPath, config);
        // config = generateDataSetConfigs(name, configPath, config);
        // config = generateFilterDefaultConfigs(name, configPath, config);
        config = generateURLS(name, configPath, config)
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
                config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]));
                config[Object.keys(config)[1]].forEach(item => {
                    if (typeof item === 'object') clearConfig(item);
                });
            } else {
                config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]));
                if (typeof config[name] === 'object') clearConfig(config[name]);
            }
        }
        // write the config to the data source config file only if it has changed
        if (!_.isEqual(config, originalConfig)) {
            fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
        }
    });
}

function generateDataSourceConfigs(name, configPath, config) {
    if (configPath.includes('mapping/datasources.json')) {
        if (!config.includes(name)) config.push(name);
    }
    return config;
}

function generateDataSetConfigs(name, configPath, config) {
    if (configPath.includes('mapping/datasets.json')) {
        if (!Object.keys(config).includes(name)) {
            config[name] = JSON.parse(JSON.stringify(config[Object.keys(config)[0]]));
            // for each key, set the value to false
            for (let key in config[name]) {
                config[name][key] = false;
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

function generateURLS(name, configPath, config) {
    if (configPath.includes('urls/index.json')) {
        if (!Object.keys(config).includes(name)) {
            config[name] = "http://localhost:4004/data/" + name
        }
    }
    return config
}

// Function to remove string values from the created config, where obj is the config object with the key selected.
// We could introduce a "field name filter", for example allowing all OData api filtering and value names to remain in the config.
function clearConfig(obj) {
    Object.keys(obj).forEach(key => {
        // if the data is an array, check each element if they are an object.
        if (Array.isArray(obj[key])) {
            obj[key].forEach(item => {
                if (typeof item === 'object') clearConfig(item);
            })
            // if the element is an object, process the object's keys.
        } else if (typeof obj[key] === 'object') {
            clearConfig(obj[key])
            // if the element is a string, remove the string value.
        } else {
            obj[key] = ''
        }
    });
}
