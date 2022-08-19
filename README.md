# Getting Started
File or Folder | Purpose
---------|----------
`app/` | javascript to support data processing
`db/` | domain models and data go here
`srv/` | service models go here
`package.json` | project metadata and configuration
`readme.md` | readme
`server.js` | javascript to override default server behaviour

## Requirements
Install the local packages
- `npm install`
- run with `npm start`

Note: if you want to manually control the running of the API, 
- install cds once with `sudo npm i --global @sap/cds-dk`
- run with `cds watch`

## Create a .env file containing
```
DATA_EXPLORER_SERVER=<Where your "The Data Explorer Server" is installed>
```

## Running

- Open a new terminal and run `cds watch` 

## Demo calls
[Budget aggregation](http://localhost:4004/data/IATIBudget?$apply=groupby((budget_value_currency),aggregate(budget_value%20with%20sum%20as%20amount)))

## Dev notes
[](https://cap.cloud.sap/docs/guides/databases)
provided files need to be in the pattern of namespace-entity.extension, where namespace in current development is 'data'

[](https://cap.cloud.sap/docs/node.js/cds-serve#cdsonce--bootstrap-expressjs-app)
This is how we hook into our filesystem, meaning that if we update the files in the /db/data folder, the app reloads and creates an api endpoint for this new file

## Datasets
### The global fund data
The Global Fund is the original datasource for the data explorer, and it would make sense to use as part of the sample datasets.

### IATI.cloud
The IATI.cloud dataset is one we host ourselves and know how to use to build proper front-ends.

### HXL data
Data searched for using [humdata search](https://data.humdata.org/dataset?vocab_Topics=hxl&sort=total_res_downloads%20desc#dataset-filter-start)
[who data per country](https://data.humdata.org/dataset?vocab_Topics=hxl&q=budget&sort=total_res_downloads%20desc&ext_page_size=25)

[Palestine](https://data.humdata.org/dataset/fts-requirements-and-funding-data-for-occupied-palestinian-territory) data, csv.
[Ukraine](https://data.humdata.org/dataset/fts-requirements-and-funding-data-for-ukraine) data, csv.
[acaps](https://data.humdata.org/dataset/acaps-covid19-government-measures-dataset)
[marawi](https://data.humdata.org/dataset/philippines-who-is-doing-what-where-in-marawi-conflict)

## Used / Useful packages
[csvtojson](https://www.npmjs.com/package/csvtojson) for converting csv files to json
[xml2json](https://www.npmjs.com/package/xml2json) for converting xml files to json
[@sap/cds](https://www.npmjs.com/package/@sap/cds) for editing the cds server behaviour
[type-detect](https://www.npmjs.com/package/type-detect) for detecting data types better than the build in typeof
[moment](https://www.npmjs.com/package/moment) for detecting dates from strings

## Datasource requirements
- A maximum filesize of 35 MB Should be respected. In most cases, it will work (slowly), but larger than 40mb XLSX files cannot be processed.
- A provided datasource MUST be a correct file, otherwise data may not be served as expected. Incorrect files include sections with additional information and columns with many different data types.
- Providing a field named 'ID' is not preferred, as it should be the ID integer for the internal API
- For CSV files, a header row is required
- Capability to process HXL files is included, but the filename MUST include HXL, to ensure processing

## Additional SAP Cloud information
Learn more at [](https://cap.cloud.sap/docs/get-started/).
