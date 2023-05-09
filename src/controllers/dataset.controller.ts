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

    const solrPostStatus = await _postDatasetToSolr(name, fileName);
    if (solrPostStatus !== 'Success') {
      return { error: solrPostStatus };
    }

    try {
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
  const options = {
    host: 'localhost',
    port: '8983',
    path: `/solr/admin/cores?action=CREATE&name=${name}&configSet=_default`,
    method: 'GET',
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
  const solrPostOptions = ` -c ${name} ${filePath}`;

  const postCommand = solrPostBase + solrPostOptions;
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
  const options = {
    host: 'localhost',
    port: '8983',
    path: `/solr/admin/cores?action=UNLOAD&core=${name}`,
    method: 'GET',
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
  // http://localhost:8983/solr/dx645a528dc96e68a62f4c5c09/select?indent=true&q.op=OR&q=*%3A*&useParams=
  const dsObj = {
      "id": name.substring(2),
      "datasource": "solr",
      "url": "http://localhost:8983/solr/" + name + "/select?indent=true&q.op=OR&q=*%3A*&useParams=",
      "countUrl": "http://localhost:8983/solr/" + name + "/select?indent=true&q.op=OR&q=*%3A*&useParams=&rows=0",
  };
  additionalDatasets.push(dsObj);
  fs.writeFileSync(datascraperDatasetsLoc, JSON.stringify(additionalDatasets, null, 2));
}
