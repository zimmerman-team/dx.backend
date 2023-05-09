"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FileController = void 0;
const tslib_1 = require("tslib");
const rest_1 = require("@loopback/rest");
const http_1 = tslib_1.__importDefault(require("http"));
const util_1 = require("util");
const child_process_1 = require("child_process");
const fs_extra_1 = tslib_1.__importDefault(require("fs-extra"));
class FileController {
    async processFile(fileName) {
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
            await fs_extra_1.default.unlink(process.env.STAGING_DIR + fileName);
            console.debug(`File '${fileName}' removed`);
        }
        catch (error) {
            console.error(`Error removing file '${fileName}': ${error}`);
        }
        // add a data scraper entry
        _addSSRDataScraperEntry(name);
        return { fileName };
    }
    ;
    async deleteDataset(fileName) {
        console.log(`Deleting dataset: ${fileName}`);
        const name = fileName.split('.')[0];
        const coreDeletionStatus = await _deleteSolrCore(name);
        if (coreDeletionStatus !== 'Success') {
            return { error: coreDeletionStatus };
        }
        return { fileName };
    }
    ;
}
tslib_1.__decorate([
    (0, rest_1.post)('/upload-file/{fileName}'),
    tslib_1.__param(0, rest_1.param.path.string('fileName')),
    tslib_1.__metadata("design:type", Function),
    tslib_1.__metadata("design:paramtypes", [String]),
    tslib_1.__metadata("design:returntype", Promise)
], FileController.prototype, "processFile", null);
tslib_1.__decorate([
    (0, rest_1.post)('/delete-dataset/{fileName}'),
    tslib_1.__param(0, rest_1.param.path.string('fileName')),
    tslib_1.__metadata("design:type", Function),
    tslib_1.__metadata("design:paramtypes", [String]),
    tslib_1.__metadata("design:returntype", Promise)
], FileController.prototype, "deleteDataset", null);
exports.FileController = FileController;
;
const _createSolrCore = async (name) => {
    const options = {
        host: 'localhost',
        port: '8983',
        path: `/solr/admin/cores?action=CREATE&name=${name}&configSet=_default`,
        method: 'GET',
    };
    return new Promise((resolve) => {
        const req = http_1.default.request(options, (res) => {
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
const _postDatasetToSolr = async (name, fileName) => {
    const solrPostBase = process.env.SOLR_POST_PATH;
    const filePath = process.env.STAGING_DIR + fileName;
    const solrPostOptions = ` -c ${name} ${filePath}`;
    const postCommand = solrPostBase + solrPostOptions;
    const execPromise = (0, util_1.promisify)(child_process_1.exec);
    try {
        const { stdout, stderr } = await execPromise(postCommand);
        console.debug(`stdout: ${stdout}`);
        if (stderr) {
            console.debug(`stderr: ${stderr}`);
            return 'Failed to post dataset to Solr';
        }
        console.debug(`_postDatasetToSolr DONE: Success`);
        return 'Success';
    }
    catch (err) {
        console.error(`Error: ${err}`);
        return 'Failed to post dataset to Solr';
    }
};
const _deleteSolrCore = async (name) => {
    const options = {
        host: 'localhost',
        port: '8983',
        path: `/solr/admin/cores?action=UNLOAD&core=${name}`,
        method: 'GET',
    };
    return new Promise((resolve) => {
        const req = http_1.default.request(options, (res) => {
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
function _addSSRDataScraperEntry(name) {
    // Update SSR with an additional dataset for the SSR parser to process.
    // load the additional datasets json list
    const datascraperDatasetsLoc = process.env.DATA_EXPLORER_SSR + "additionalDatasets.json";
    let additionalDatasets = JSON.parse(fs_extra_1.default.readFileSync(datascraperDatasetsLoc, 'utf8'));
    // check if the name is already in the ids of the loaded list
    for (const dataset of additionalDatasets) {
        if (dataset.id === name.substring(2))
            return;
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
    fs_extra_1.default.writeFileSync(datascraperDatasetsLoc, JSON.stringify(additionalDatasets, null, 2));
}
//# sourceMappingURL=dataset.controller.js.map