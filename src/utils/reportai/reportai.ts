
import { promisify } from 'util';
import { exec } from 'child_process';
import fs from 'fs-extra';


import axios from 'axios';
import FormData from 'form-data';

import { reportBase, rowItemChart, oneToFourBase, fourToOneBase, chartBaseBarChart, chartBaseLineChart, chartBaseGeoMap } from './consts';

export const _searchKaggle = async (searchTerm: string) => {
  /**
   * Use the `kaggle` tool to search for datasets on Kaggle.
   * If datasets are found, store and return the refs for the datasets
   */
  console.debug('DEBUG:: Searching Kaggle for datasets');
  const kaggleSearch = `kaggle datasets list --file-type csv -s ${searchTerm} --csv --max-size 40000000`;
  const execPromise = promisify(exec);
  try {
    const { stdout, stderr } = await execPromise(kaggleSearch);
    if (stderr) return [];
    // the content of stdout is a csv string
    // parse it into an array of objects
    const rows = stdout.split('\n');
    const firstColumnValues = [];
    for (let i = 1; i < rows.length - 1; i++) {  // -1 because the last row is empty
      if (i > 3) break;
      const values = rows[i].split(',');
      const firstColumnValue = values[0];
      firstColumnValues.push(firstColumnValue);
    }
    return firstColumnValues;
  } catch (err) {
    console.error(`Error: ${err}`);
    return [];
  }
};

export const _getMetadata = async (kaggleDatasets: string[]) => {
  /**
   * Use the `kaggle` tool to get the metadata for each dataset in kaggleDatasets
   * If metadata is found, store and return the metadata for each dataset
   */
  console.debug('DEBUG:: Retrieving dataset metadata from Kaggle');
  const execPromise = promisify(exec);
  let metadata: any = {};
  for (const datasetRef of kaggleDatasets) {
    // create a cleanedRef that only has letters and numbers
    const metadataGetter = `kaggle datasets metadata ${datasetRef} --path ./staging/`
    try {
      const { stdout, stderr } = await execPromise(metadataGetter);
      if (stderr) return [];
      if (!stdout) return [];
      const metadataLoc = `./staging/dataset-metadata.json`;
      const jsonMetadata = JSON.parse(fs.readFileSync(metadataLoc, 'utf8'));
      metadata[datasetRef] = {
        'title': jsonMetadata['title'],
        'subtitle': jsonMetadata['subtitle'],
        'description': jsonMetadata['description'],
        'url': `https://www.kaggle.com/${jsonMetadata['id']}`
      };
      fs.removeSync(metadataLoc);
    } catch (err) {
      console.error(`Error: ${err}`);
    }
  }

  return metadata;
};

export const _checkDatasetFiles = async (datasetRef: string) => {
  console.debug('DEBUG:: Retrieving dataset files from Kaggle');
  const execPromise = promisify(exec);
  const kaggleDownload = `kaggle datasets files -v ${datasetRef}`;
  try {
    const { stdout, stderr } = await execPromise(kaggleDownload);
    if (stderr) {
      return [];
    }
    const rows = stdout.split('\n');
    const datasetFileNames = [];
    for (let i = 1; i < rows.length - 1; i++) {  // -1 because the last row is empty
      const values = rows[i].split(',');
      const firstColumnValue = values[0];
      datasetFileNames.push(firstColumnValue);
    }
    return datasetFileNames;
  } catch (err) {
    console.error(`Error: ${err}`);
    return [];
  }
};

export const _downloadDataset = async (datasetRef: string, datasetFileNames: string[]) => {
  console.debug('DEBUG:: Downloading datasets from Kaggle');
  const execPromise = promisify(exec);
  const kaggleDownload = `kaggle datasets download --path ./staging/ ${datasetRef} --unzip`;
  try {
    const { stdout, stderr } = await execPromise(kaggleDownload);
    if (!stdout) return false;
    if (stderr) {      
      // check if all the files in datasetFileNames are in the staging directory
      const stagingDir = './staging/';
      const stagingFiles = fs.readdirSync(stagingDir);
      for (const fileName of datasetFileNames) {
        if (!stagingFiles.includes(fileName)) return false;
      }
      return true;
    }
    return true;
  } catch (err) {
    console.error(`Error: ${err}`);
    return false;
  }
};

export const _createDatasets = async (datasetFileNames: string[], datasetMetadata: any) => {
  console.debug('DEBUG:: Creating datasets in DX Middleware');
  let datasetIds: any = {};
  for (const fileName of datasetFileNames) {
    const datasetInfo = {
      "name": fileName.split('.')[0],
      "description": `Dataset retrieved from: ${datasetMetadata['url']}`,
      "public": true,
      "category": "Other",
    };
    const res = await axios.post(`http://localhost:4200/datasets`, datasetInfo);
    datasetIds[fileName] = res.data.id; 
  };
  return datasetIds;
};

export const _renameDatasets = async (datasetIds: any) => {
  try {
    // each dataset in datasetIds is a key-value pair of fileName: datasetId
    // rename each file at ./staging/ dataset to the respective datasetId, 
    // but only if the filename is one of the keys in datasetIds
    const newFileNames = [];
    const stagingDir = './staging/';
    const stagingFiles = fs.readdirSync(stagingDir);
    for (const fileName of stagingFiles) {
      if (fileName in datasetIds) {
        const newName = `dx${datasetIds[fileName]}.csv`
        fs.renameSync(stagingDir + fileName, stagingDir + newName);
        newFileNames.push(newName)
      }
    }

    return newFileNames;
  } catch (err) {
    console.error(`Error renaming the datasets:\n${datasetIds}\nerror:\n${err}`);
    return [];
  }
};

export const _postFilesToSolr = async (fileNames: string[]) => {
  console.debug('DEBUG:: Posting files to Solr and updating DX SSR');
  let results: boolean[] = [];
  for (const fileName of fileNames) {
    // create a copy of the file at ./staging/fileName
    const stagingDir = './staging/';
    fs.copyFileSync(stagingDir + fileName, stagingDir + 'AI_' + fileName);
    await axios.post(`http://localhost:4004/upload-file/${fileName}`)
      .then(_ => results.push(true))
      .catch(err => {
        console.log(err)
        results.push(false);
      });
  }
  console.debug('DEBUG:: PostFilesToSolr:: Solr update complete');
  await axios.get(`http://localhost:4400/trigger-update`)
    .then(_ => console.debug("DEBUG:: PostFilesToSolr:: SSR update complete"))
    .catch(_ => {console.debug("DEBUG:: PostFilesToSolr:: SSR update failed")});
  return results;
};

export const _generateChartsAndReport = async (newFileNames: string[], datasetMetadata: any) => {
  console.debug('DEBUG:: Start report generation');
  if (newFileNames.length === 0) return '';
  
  const fileNames = newFileNames.map(fileName => `AI_${fileName}`);
  // for every file, create a chart.
  const createdCharts = [];
  for (const fileName of fileNames) {
    // update the first line (separated by \n) with .replace(/dx.*?__/g, '')
    const stagingDir = './staging/';
    const fileContent = fs.readFileSync(stagingDir + fileName, 'utf8');
    const lines = fileContent.split('\n');
    lines[0] = lines[0].replace(/dx.*?__/g, '');
    fs.writeFileSync(stagingDir + fileName, lines.join('\n'));
    const charts = await _AGIAPI(fileName);
    for (const chart of charts) {
      console.log("pushing in chart ids", chart.chartId)
      createdCharts.push(chart);
    }
  }
  // create a report with all the charts.
  const reportId = await _createReport(createdCharts, datasetMetadata);
  return reportId;
};

export const _AGIAPI = async (fileName: string) => {
  // axios post to localhost:5000/chart-suggest/csv-dataset with form data 'file' as ./staging/filename
  const url = `http://localhost:5000/chart-suggest/ai-report-builder`;
  const formData = new FormData();
  formData.append('file', fs.createReadStream(`./staging/${fileName}`));
  const response = await axios.post(url, formData, {
    headers: formData.getHeaders(),
  });
  let charts: any[] = [];
  const chartInputs = response.data.result;
  for (const chartInput of chartInputs) {
    let chart: any = {};
    if (response.data.code === 200) {
      chart = await _createChartWithUploadedData(fileName, chartInput);
      console.log("AGIAPI createChart ID", chart.chartId)
      if (chart.chartId !== '') charts.push(chart);
    }
  }
  return charts;
}

export const _createChartWithUploadedData = async (fileName: string, data: any) => {
  console.debug('DEBUG:: Report Generation:: Generating Charts');
  const start = data.indexOf('{');
  const end = data.lastIndexOf('}');
  const jsonString = data.substring(start, end + 1);

  let explanation = '';
  let chartContent = {};
  try {
    const jsonObject = JSON.parse(jsonString);
    let chart = _createChart(fileName, jsonObject, jsonObject.chartType);
    if (chart.error === 'Not Implemented') return {chartId: '', explanation: ''}
    explanation = chart.explanation
    chartContent = chart.chartContent
  } catch (error) {
    console.error('Invalid JSON string:', error);
  }

  let ret = {
    chartId: '',
    explanation: explanation,
  };

  await axios.post(`http://localhost:4200/chart`, chartContent)
    .then(res => {
      ret['chartId'] = res.data.id;
    })
    .catch(err => console.log("Chart creation failed:", err));
  return ret;
};

export const _createReport = async (charts: any, datasetMetadata: any) => {
  console.debug('DEBUG:: Report Generation:: Creating Report');
  let report = { ...reportBase };
  report.name = datasetMetadata.title;
  report.title = datasetMetadata.title;
  const datasetSource = `\n\nThe source of this dataset is ${datasetMetadata.url}`;
  report.subTitle.blocks[0].text = datasetMetadata.subtitle + datasetSource;

  let reportRows = [];

  let leftRightBool = true;
  for (const chart of charts) {
    if (chart.explanation === '') {
      let chartItem = JSON.parse(JSON.stringify(rowItemChart));
      chartItem.items[0] = chart.chartId;
      reportRows.push(chartItem);
    } else {
      const base = leftRightBool ? oneToFourBase : fourToOneBase;
      const chartIndex = leftRightBool ? 1 : 0;
      const textIndex = leftRightBool ? 0 : 1;
      leftRightBool = !leftRightBool;
      let rowItem = JSON.parse(JSON.stringify(base));
      // text:
      rowItem.items[textIndex].blocks[0].text = chart.explanation;
      rowItem.items[chartIndex] = chart.chartId;
      reportRows.push(rowItem);
    }
  }

  report.rows = reportRows;
  let reportId = '';
  await axios.post(`http://localhost:4200/report`, report)
    .then(res => {
      console.log("Report created, res:", res.data.id)
      reportId = res.data.id;
    })
    .catch(err => console.log("Report creation failed:", err));
  
  return reportId;
}

export const _createChart = (fileName: string, jsonObject: any, chartType: string) => {
  console.debug('DEBUG:: Report Generation:: Creating Chart');
  let chartContent: any;

  // Create a base chart and set the chartType specific values
  switch (chartType) {
    case 'barchart':
      chartContent = JSON.parse(JSON.stringify(chartBaseBarChart));
      chartContent.mapping.bars.value[0] = jsonObject['bars'];
      break;
    case 'linechart':
      chartContent = JSON.parse(JSON.stringify(chartBaseLineChart));
      chartContent.mapping.x.value[0] = jsonObject['x'];
      chartContent.mapping.y.value[0] = jsonObject['y'];
      chartContent.mapping.lines.value[0] = jsonObject['lines'];
      break;
    // case 'sankey':
    //   chartContent = { ...chartBaseSankey };
    //   break;
    case 'geomap':
      chartContent = JSON.parse(JSON.stringify(chartBaseGeoMap));
      chartContent.mapping.country.value[0] = jsonObject['country'];
      break;
    default:
      // Handle unknown chart types
      return { chartContent: {}, explanation: '', error: 'Not Implemented'};
  }
  // Set the general values
  chartContent.name = 'AI Generated chart';
  if ('explanation' in jsonObject) chartContent.name = jsonObject['explanation'];
  if ('title' in jsonObject) chartContent.name = jsonObject['title'];
  chartContent.datasetId = fileName.split('.')[0].replace('AI_dx', '');
  let filterOptions: string[] = []
  if (Object.keys(jsonObject['size'])) filterOptions = Object.keys(jsonObject['size']);
  chartContent.enabledFilterOptionGroups = filterOptions;

  // Set the size values
  // can become a switch later on
  const firstKey = Object.keys(jsonObject['size'])[0];

  if (chartType !== 'linechart') {
    chartContent.mapping.size.value[0] = firstKey;
    chartContent.mapping.size.config.aggregation[0] = jsonObject['size'][firstKey]
  }

  const explanation = ('explanation' in jsonObject) ? jsonObject['explanation'] : '';
  return { chartContent: chartContent, explanation: explanation };
}

