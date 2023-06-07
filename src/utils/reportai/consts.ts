/** CHARTS */
/**
 * Values to be entered:
 * Gemeral:
 *  name
 *  datasetId
 *  enabledFilterOptionGroups
 * Bars:
 *  mapping.bars.value[0]
 *  mapping.bars.mappedType
 * Size:
 *  mapping.size.value[0]
 *  mapping.size.mappedType
 *  mapping.size.config.aggregation[0]
 * 
 */
export const chartBaseBarChart = {
    "name": "My first chart",
    "vizType": "echartsBarchart",
    "mapping": {
        "bars": {
            "ids": [
                "1"
            ],
            "value": [
                "" // to be filled
            ],
            "isValid": true,
            "mappedType": "number"
        },
        "size": {
            "ids": [
                "2"
            ],
            "value": [
                "" // to be filled
            ],
            "isValid": true,
            "mappedType": "number",
            "config": {
                "aggregation": [
                    "" // to be filled with one of: average, count, count unique, max, min, sum, median
                ]
            }
        }
    },
    "datasetId": "", // to be filled
    "vizOptions": {
        "width": 576,
        "height": 1108,
        "background": "#FFFFFF",
        "marginTop": 20,
        "marginRight": 10,
        "marginBottom": 50,
        "marginLeft": 120,
        "barWidth": 80,
        "stack": false,
        "showTooltip": true,
        "isMonetaryValue": false,
        "label": true
    },
    "appliedFilters": {},
    "enabledFilterOptionGroups": [''] // to be filled with available fields
  }
  
  /**
   * Values to be entered:
   * Gemeral:
   *  name
   *  datasetId
   *  enabledFilterOptionGroups
   * country:
   *  mapping.country.value[0]
   * Size:
   *  mapping.size.value[0]
   *  mapping.size.config.aggregation[0]
   * 
   */
  export const chartBaseGeoMap = {
    "name": "AI Generated Chart",
    "vizType": "echartsGeomap",
    "mapping": {
        "country": {
            "ids": [
                "1"
            ],
            "value": [
                ""
            ],
            "isValid": true,
            "mappedType": "string"
        },
        "size": {
            "ids": [
                "2"
            ],
            "value": [
                ""
            ],
            "isValid": true,
            "mappedType": "number",
            "config": {
                "aggregation": [
                    "sum"
                ]
            }
        }
    },
    "datasetId": "",
    "vizOptions": {
        "width": 1213,
        "height": 1115,
        "background": "#FFFFFF",
        "marginTop": 0,
        "marginRight": 0,
        "marginBottom": 0,
        "marginLeft": 0,
        "roam": "none",
        "scaleLimitMin": 1,
        "scaleLimitMax": 1,
        "showTooltip": true,
        "isMonetaryValue": false
    },
    "appliedFilters": {},
    "enabledFilterOptionGroups": []
  }
  
  /**
   * Values to be entered:
   * Gemeral:
   *  name
   *  datasetId
   *  enabledFilterOptionGroups
   * x:
   *  mapping.x.value[0]
   * y:
   *  mapping.y.value[0]
   *  mapping.y.config.aggregation[0]
   * 
   */
  export const chartBaseLineChart = {
    "name": "AI Generated LineChart",
    "vizType": "echartsLinechart",
    "mapping": {
      "x": {
        "ids": [
            "1"
        ],
        "value": [
            ""
        ],
        "isValid": true,
        "mappedType": "number"
      },
      "y": {
        "ids": [
          "2"
        ],
        "value": [
          ""
        ],
        "isValid": true,
        "mappedType": "number",
        "config": {
          "aggregation": [
            "sum"
          ]
        }
      },
      "lines": {
        "ids": [
          "3"
        ],
        "value": [
          ""
        ],
        "isValid": true,
        "mappedType": "string"
      }
    },
    "datasetId": "",
    "vizOptions": {
        "width": 1213,
        "height": 1115,
        "background": "#FFFFFF",
        "marginTop": 20,
        "marginRight": 10,
        "marginBottom": 50,
        "marginLeft": 120,
        "lineType": "solid",
        "lineWidth": 1,
        "stack": false,
        "showArea": true,
        "showLegend": true,
        "legendHoverLink": false,
        "showTooltip": true,
        "isMonetaryValue": false,
        "label": true
    },
    "appliedFilters": {},
    "enabledFilterOptionGroups": ['']
  }
  
  /** REPORT SECTIONS */
  export const rowItemChart = {
    "structure": "oneByOne",
    "items": [
        "647dcb2f11d5c3c949e19f9a"
    ]
  };
  
  export const rowItemText = {
    "structure": "oneByOne",
    "items": [
        {
            "blocks": [
                {
                    "key": "5klvm",
                    "text": "Text!",
                    "type": "unstyled",
                    "depth": 0,
                    "inlineStyleRanges": [],
                    "entityRanges": [],
                    "data": {}
                }
            ],
            "entityMap": {}
        }
    ]
  }
  
  /**
   * Values to be entered:
   * General:
   *  name
   *  title
   *  subTitle.blocks[0].text
   * Rows:
   *  for every chart add:
   *   rowItemChart
   *   edit:
   *     items[0] with the chart id
   *   rowItemText
   *   edit:
   *     items[0].blocks[0].text with the chart explanation
   */
  export const reportBase = {
    "name": "AI Report",
    "showHeader": true,
    "title": "AI Report title",
    "subTitle": {
        "blocks": [
            {
                "key": "gi4v",
                "text": "AI Report description",
                "type": "unstyled",
                "depth": 0,
                "inlineStyleRanges": [],
                "entityRanges": [],
                "data": {}
            }
        ],
        "entityMap": {}
    },
    "rows": [
        rowItemText,
        rowItemChart
    ],
    "backgroundColor": "#252c34",
    "titleColor": "#ffffff",
    "descriptionColor": "#ffffff",
    "dateColor": "#ffffff"
  }
  
  export const oneToFourBase = {
    "structure": "oneToFour",
    "items": [
      {
        "blocks": [
          {
            "key": "b68he",
            "text": "Text!",
            "type": "unstyled",
            "depth": 0,
            "inlineStyleRanges": [],
            "entityRanges": [],
            "data": {}
          }
        ],
        "entityMap": {}
      },
      "id"
    ]
  }
  
  export const fourToOneBase = {
    "structure": "fourToOne",
    "items": [
      "id",
      {
        "blocks": [
          {
            "key": "2fgun",
            "text": "Text!",
            "type": "unstyled",
            "depth": 0,
            "inlineStyleRanges": [],
            "entityRanges": [],
            "data": {}
          }
        ],
        "entityMap": {}
      }
    ]
  }