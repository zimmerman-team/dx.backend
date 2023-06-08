import {inject} from '@loopback/core';
import {
  Request,
  RestBindings,
  post,
  param,
} from '@loopback/rest';
import axios from 'axios';
import FormData from 'form-data';

import { _searchKaggle, _getMetadata, _checkDatasetFiles, _downloadDataset, _createDatasets, _renameDatasets, _postFilesToSolr, _generateChartsAndReport } from '../utils/reportai/reportai';

/**
 * A simple controller to bounce back http requests
 */
export class ReportAIController {
  constructor(@inject(RestBindings.Http.REQUEST) private req: Request) {}

  @post('/report-ai/search/{topic}')
  async searchKaggle(
    @param.path.string('topic') topic: string,
  ) {
    let searchTerm = "";
    const url = 'http://localhost:5000/prompts/extract-search-term';
    const formData = new FormData();
    formData.append('prompt', topic);
    await axios.post(url, formData, {headers: formData.getHeaders()})
      .then((response: any) => {
        if (response.data.code === 200) searchTerm = response.data.result;
      })
      .catch((error: any) => {
        console.log(error);
      });
    if (searchTerm === '') 
        return { 'result': 'Oops, we cannot find datasets for this search term, try a different one!' };
    const kaggleDatasets = await _searchKaggle(searchTerm); // refs of the datasets
    if (kaggleDatasets.length === 0)
        return { 'result': 'Oops, we cannot find datasets for this search term, try a different one!' };

    const metadata = await _getMetadata(kaggleDatasets); // metadata from the chosen datasets from kaggle
    return metadata;
  }

  @post('/report-ai/create-report/{datasetRef}')
  async datasetRef(
    @param.path.string('datasetRef') datasetRef: string,
  ) {
    const kaggleDatasets = [datasetRef]; // refs of the datasets
    if (kaggleDatasets.length === 0)
      return { 'result': 'Oops, we cannot find datasets for this search term, try a different one!' };

    const metadata = await _getMetadata(kaggleDatasets); // metadata from the chosen datasets from kaggle
    // user selects dataset 1.
    const datasetMetadata = metadata[datasetRef]; // datasetMetadata has the title, subtitle, description and url
    const datasetFileNames = await _checkDatasetFiles(datasetRef); // check the files for the dataset
    if (datasetFileNames.length === 0)
      return { 'result': 'Oops, this dataset does not have any files attached, try a different one!' };
    const downloadSuccess = await _downloadDataset(datasetRef, datasetFileNames); // download the files for the dataset
    if (!downloadSuccess)
      return { 'result': "Oops, we cannot currently download this dataset's files, try a different dataset!" };
    
    // create a dataset using the middleware. For each dataset in datasetFileNames, create a dataset.
    const datasetIds = await _createDatasets(datasetFileNames, datasetMetadata); // create a middleware dataset
    const newFileNames = await _renameDatasets(datasetIds); // rename the files to the datasetIds
    if (newFileNames.length === 0)
      return { 'result': 'Oops, we were not able to convert the file names, try a different dataset!' };

    // trigger the solr update.
    const postToSolrResults = await _postFilesToSolr(newFileNames); // post the files to solr and update SSR.
    if (postToSolrResults.includes(false))
      return { 'result': 'Oops, we were not able to post the files to Solr, try a different dataset!' };

    const reportId: string = await _generateChartsAndReport(newFileNames, datasetMetadata);
    if (reportId === '')
      return { 'result': 'Oops, we were not able to create the report, try a different dataset!' };
    return { 'url': 'http://localhost:3000/report/' + reportId };
  }
}

