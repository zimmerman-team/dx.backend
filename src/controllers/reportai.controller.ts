import {inject} from '@loopback/core';
import {
  Request,
  RestBindings,
  post,
  param,
} from '@loopback/rest';
import { Configuration, OpenAIApi } from "openai";

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
    const configuration = new Configuration({
        apiKey: process.env.OPENAI_API_KEY,
    });
    const openai = new OpenAIApi(configuration);
    
    const topicDeterminer = "First, I will give you some context, then I will ask you a question about a given topic. The context: Your job is to convert the given topic with one search term which will lead to functional results on the Kaggle datasets page. Keep it as concise as possible. Do not include the word 'dataset' or 'report' in your answer. If the best answer has multiple words, append them together with a + symbol. The Topic is: '" + topic + "'. The search term is: ";
    const response = await openai.createCompletion({
        model: "text-davinci-003",
        prompt: topicDeterminer,
        temperature: 0.08,
        max_tokens: 256,
        top_p: 1,
        frequency_penalty: 0,
        presence_penalty: 0,
    });
    const searchTerm = response.data.choices[0].text?.replace(/\n/g, '').replace(/ /g, '').replace(/dataset/g, '').replace(/report/g, '') ?? '';

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

