import { post, param } from '@loopback/rest';
import http from 'http';
import { promisify } from 'util';
import { exec } from 'child_process';
import fs from 'fs-extra';

export class FileController {
  @post('/upload-file/{fileName}')
  async processFile(
    @param.path.string('fileName') fileName: string,
  ) {
    console.log(`Processing file: ${fileName}`);

    const name = fileName.split('.')[0];
    const coreCreationStatus = await _createSolrCore(name);
    if (coreCreationStatus !== 'Success') {
      return { error: coreCreationStatus };
    }

    const datasetPrepStatus = await _preprocessCSV(fileName);
    if (datasetPrepStatus !== 'Success') {
      return { error: datasetPrepStatus };
    }

    const solrPostStatus = await _postDatasetToSolr(name, fileName);
    if (solrPostStatus !== 'Success') {
      return { error: solrPostStatus };
    }

    try {
      fs.copyFileSync(process.env.STAGING_DIR + fileName, process.env.STAGING_DIR + 'AI_' + fileName);
      await fs.unlink(process.env.STAGING_DIR + fileName);
      console.debug(`File '${fileName}' removed`);
    } catch (error) {
      console.error(`Error removing file '${fileName}': ${error}`);
    }

    // add a data scraper entry
    _addSSRDataScraperEntry(name);

    return { fileName };
  };

  @post('/delete-dataset/{fileName}')
  async deleteDataset(
    @param.path.string('fileName') fileName: string,
  ) {
    console.log(`Deleting dataset: ${fileName}`);
    const name = fileName.split('.')[0];
    const coreDeletionStatus = await _deleteSolrCore(name);
    if (coreDeletionStatus !== 'Success') {
      return { error: coreDeletionStatus };
    }
    return { fileName }
  };
};

const _createSolrCore = async (name: string) => {
  const host = process.env.SOLR_SUBDOMAIN ? `dx-solr` : 'localhost';
  const options = {
    host: host,
    port: '8983',
    path: `/solr/admin/cores?action=CREATE&name=${name}&configSet=_default`,
    method: 'GET',
    auth: `${process.env.SOLR_ADMIN_USERNAME}:${process.env.SOLR_ADMIN_PASSWORD}`,
  };

  return new Promise<string>((resolve) => {
    const req = http.request(options, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        console.debug(chunk);
      });
      res.on('end', () => {
        console.debug(`_createSolrCore DONE: Success`);
        resolve('Success');
      });
    });
    req.on('error', (e) => {
      console.error(`Problem with request: ${e.message}`);
      resolve('Failed to create core');
    });
    req.end();
  });
};

const _postDatasetToSolr = async (name: string, fileName: string) => {
  const solrPostBase = process.env.SOLR_POST_PATH;
  const filePath = process.env.STAGING_DIR + fileName;
  const auth = process.env.SOLR_ADMIN_USERNAME + ':' + process.env.SOLR_ADMIN_PASSWORD;
  let host = process.env.SOLR_SUBDOMAIN ? `${auth}@dx-solr` : 'localhost';
  if (auth !== ':' && host === 'localhost') host = `${auth}@localhost`
  const postCommand = solrPostBase + ` -url 'http://${host}:8983/solr/${name}/update' ${filePath}`;
  const execPromise = promisify(exec);

  try {
    const { stdout, stderr } = await execPromise(postCommand);
    console.debug(`stdout: ${stdout}`);
    if (stderr) {
      console.debug(`stderr: ${stderr}`);
      return 'Failed to post dataset to Solr';
    }
    console.debug(`_postDatasetToSolr DONE: Success`);
    return 'Success';
  } catch (err) {
    console.error(`Error: ${err}`);
    return 'Failed to post dataset to Solr';
  }
};

const _deleteSolrCore = async (name: string) => {
  const host = process.env.SOLR_SUBDOMAIN ? 'dx-solr' : 'localhost';
  const options = {
    host: host,
    port: '8983',
    path: `/solr/admin/cores?action=UNLOAD&core=${name}`,
    method: 'GET',
    auth: `${process.env.SOLR_ADMIN_USERNAME}:${process.env.SOLR_ADMIN_PASSWORD}`,
  };
  return new Promise<string>((resolve) => {
    const req = http.request(options, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        console.debug(chunk);
      });
      res.on('end', () => {
        console.debug(`_deleteSolrCore DONE: Success`);
        resolve('Success');
      });
    });
    req.on('error', (e) => {
      console.error(`Problem with request: ${e.message}`);
      resolve('Failed to delete core');
    });
    req.end();
  });
};


function _addSSRDataScraperEntry(name: any) {
  // Update SSR with an additional dataset for the SSR parser to process.
  // load the additional datasets json list
  const datascraperDatasetsLoc = process.env.DATA_EXPLORER_SSR + "additionalDatasets.json";
  let additionalDatasets = JSON.parse(fs.readFileSync(datascraperDatasetsLoc, 'utf8'));
  // check if the name is already in the ids of the loaded list
  for (const dataset of additionalDatasets) {
      if (dataset.id === name.substring(2)) return;
  }
  // create a dataset object and write it to the additionalDatasets json file
  const auth = `${process.env.SOLR_ADMIN_USERNAME}:${process.env.SOLR_ADMIN_PASSWORD}`;
  let host = process.env.SOLR_SUBDOMAIN ? `${auth}@dx-solr` : 'localhost';
  if (auth !== ':' && host === 'localhost') host = `${auth}@localhost`
  const dsObj = {
      "id": name.substring(2),
      "datasource": "solr",
      "url": `http://${host}:8983/solr/` + name + "/select?indent=true&q.op=OR&q=*%3A*&useParams=",
      "countUrl": `http://${host}:8983/solr/` + name + "/select?indent=true&q.op=OR&q=*%3A*&useParams=&rows=0",
  };
  additionalDatasets.push(dsObj);
  fs.writeFileSync(datascraperDatasetsLoc, JSON.stringify(additionalDatasets, null, 2));
}

const _preprocessCSV = async (fileName: string) => {
  try {
    const stagingDir = './staging/';
    // the file is a CSV file. It is comma separated. I want to read it, and change any number that is not a decimal to end with .0
    const fileContents = fs.readFileSync(stagingDir + fileName, 'utf8');
    const lines = fileContents.split('\n');
    const newLines = [];
    let isHeader = true;
    for (const line of lines) {
      // if the line is a newline, skip it
      if (line === '') continue;
      let splitLine = line;
      if (isHeader) splitLine = _removeInternalCommas(line);
      const values = splitLine.split(',');
      const newValues = [];
      for (const value of values) {
        // if (isHeader) value = replace value with just the letters and numbers in the name
        if (isHeader) {
          let newValue = value.replace(/[^a-zA-Z0-9]/g, '');
          // prefix newValue with fileName
          newValue = fileName.split('.')[0] + '__' + newValue;
          newValues.push(newValue);
        }
        // first check if the value is a number
        else if (!isNaN(Number(value))) {
          let newValue;
          if (value.includes('.')) newValue = value;
          else if (value.endsWith('\r')) newValue = value.replace(/\r$/, '.0\r');
          else newValue = value + '.0';
          newValues.push(newValue);
        } // else if the value is a string, check if it starts with an empty space and if so remove it.
        else if (value.startsWith(' ')) {
          newValues.push(value.substring(1));
        } else {
          newValues.push(value);
        }
      }
      if (isHeader) isHeader = false;
      newLines.push(newValues.join(','));
    }
    fs.writeFileSync(stagingDir + fileName, newLines.join('\n'));
    return 'Success';
  } catch (error) {
    console.error(`Error preprocessing CSV: ${error}`);
    return 'Failed to preprocess CSV';
  }
}

const _removeInternalCommas = (line: string) => {
  let insideQuotes = false;
  let result = '';

  for (const element of line) {
    if (element === '"') insideQuotes = !insideQuotes;
    if (element === ',' && insideQuotes) continue;
    result += element;
  }
  return result
}
