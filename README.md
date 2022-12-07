# Getting Started
File or Folder | Purpose
---------|----------
`app/` | javascript to support data processing
`api/db/` | domain models and data go here
`api/srv/` | service models go here
`api/server.js` | javascript to override default server behaviour
`package.json` | project metadata and configuration
`readme.md` | readme
`staging/` | staging folder which is not included by default, but created in the running command.

## Requirements
Install the local packages:
- `npm install`
- install `dx.client`, `dx.server`, `dx.ssr`.

## Create a .env file containing
```
DATA_EXPLORER_SERVER=<Where your "The Data Explorer Server" is installed + /src/config>
```

## Running
- run with: `npm start`
- starting a clean run (removing existing API mapping) with: `npm run reset`
- Note: if you want to manually control the running of the API, 
    - install cds once with: `sudo npm i --global @sap/cds-dk`
    - ensure the required directory exists: `mkdir -p staging && mkdir -p staging/db && mkdir -p staging/srv && mkdir -p staging/db/data`
    - run with: `cds watch api`

## Dev notes
[](https://cap.cloud.sap/docs/guides/databases)
provided files need to be in the pattern of namespace-entity.extension, where namespace in current development is 'data'

[](https://cap.cloud.sap/docs/node.js/cds-serve#cdsonce--bootstrap-expressjs-app)
This is how we hook into our filesystem, meaning that if we update the files in the /api/db/data folder, the app reloads and creates an api endpoint for this new file

## Used / Useful packages
[@rawgraphs/rawgraphs-core](https://www.npmjs.com/package/@rawgraphs/rawgraphs-core) for detecting data types better than the build in typeof
[@sap/cds](https://www.npmjs.com/package/@sap/cds) for editing the cds server behaviour
[csvtojson](https://www.npmjs.com/package/csvtojson) for converting csv files to json
[fs-extra](https://www.npmjs.com/package/fs-extra) for better filesystem connection
[xlsx](https://www.npmjs.com/package/xlsx) for converting XLSX files to json
[xml2json](https://www.npmjs.com/package/xml2json) for converting xml files to json

## Datasource requirements
- A maximum filesize of 35 MB Should be respected. In most cases, it will work (slowly), but larger than 40mb XLSX files cannot be processed.
- A provided datasource MUST be a correct file, otherwise data may not be served as expected. Incorrect files include sections with additional information and columns with many different data types.
- Providing a field named 'ID' is not preferred, as it should be the ID integer for the internal API. ID fields are renamed to 'datasource_id'.
- For CSV files, a header row is required.
- Capability to process HXL files is included, but the filename MUST include HXL, to ensure processing.
- CSV and XLSX Datasources must not have linebreaks, as the parser uses linebreaks to detect the next row of data.

## Additional SAP Cloud information
Learn more at [](https://cap.cloud.sap/docs/get-started/).
