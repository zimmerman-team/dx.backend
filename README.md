# Getting Started

Welcome to your new project.

It contains these folders and files, following our recommended project layout:

File or Folder | Purpose
---------|----------
`app/` | content for UI frontends goes here
`db/` | your domain models and data go here
`srv/` | your service models and code go here
`package.json` | project metadata and configuration
`readme.md` | this getting started guide


## Running

- Open a new terminal and run `cds watch` 


## Learn More

Learn more at https://cap.cloud.sap/docs/get-started/.


## Demo calls
[Budget aggregation](http://localhost:4004/data/IATIBudget?$apply=groupby((budget_value_currency),aggregate(budget_value%20with%20sum%20as%20amount)))
