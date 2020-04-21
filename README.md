[![CircleCI](https://circleci.com/gh/flywheel-apps/GRP-5-transfer-log-report.svg?style=svg)](https://circleci.com/gh/flywheel-apps/GRP-5-transfer-log-report)

# flywheel/GRP-5-transfer-log-report

[//]: # (jj - Overall, this README reads more like instructions to build the gear rather than why the gear was built and how to use it most effectively. I am unfamiliar with the intention of the gear.  So I may be totally off in my assessment.)

[//]: # (jj - How was the data transfered into the instance? Is there another "named" process for this that we reference [e.g. "The <named transfer process> is validated by performing GRP-5 with the resultant log. A report of the potential errors is given as output. See below for a detailed description of those errors and how to resolve them."] ? )
GRP-5 is a Flywheel analysis gear to validate existence of data referenced within a transfer report log within a given project. This Gear outputs a csv file with the fields: 'error', 'path',  'type', 'resolved', 'label', '_id'.

## INPUTS

[//]: # (jj - I don't know about you, but I get markdown-lint notifications for appropriate spacing around headings)

### transfer_log (required)

The transfer_log is a csv (or xls) file that describes the records that should be present in Flywheel.

### template

The template is a yaml file that describes how to map the transfer log to objects in Flywheel.
See examples/transfer-log.xlsx and transfer-log-template.yml for a transfer log and transfer log template example.

### Manifest JSON for Inputs

[//]: # (jj - This presentation of the manifest in the README.md is not a consistent theme in the GRP gears. Although helpful for someone developing gears, it seems redundant with the existence of the manifest right in this repo. As with the config section below, would it be more "concise" to reference the relevant lines [e.g. https://github.com/flywheel-apps/GRP-5-transfer-log-report/blob/master/manifest.json#L15-L37] in the manifest?)

``` json
"inputs": {
  "api-key": {
    "base": "api-key"
  },
  "template": {
    "base": "file",
    "description": "A yaml template file detailing how to read the transfer log.",
    "type": {
      "enum": [
        "source data"
      ]
    }
  },
  "transfer_log": {
    "base": "file",
    "description": "A transfer log file.",
    "type": {
      "enum": [
        "tabular data"
      ]
    }
  }
}
  ```

## Configuration Options

### case_insensitive (default = false)

If set to `true`, comparisons between the transfer log and Flywheel objects will not require case to match. Note that any regex provided in `pattern` will retain case-specificity.

Take the following template, for example:

``` yaml
query:
  - subject.label: "Subject"
  - session.label: "Timepoint"
 ```

If a subject.label in Flywheel and the transfer log Subject value were both "subject1", but session.label and "Timepoint" were week4 and Week4, respectively, they would not be considered matches unless case_insensitive was set to `true`.

### file_type (default = csv)

file_type specifies the format to which to save the output error report file (csv or json).

### filename (default = "transfer-log-report")

filename specifies the name for the output error report file.

### Manifest JSON for configuration options

``` json
"config": {
  "file_type": {
    "default": "csv",
    "description": "File Type of output report (json or csv).",
    "type": "string",
    "enum": [
        "csv",
        "json"
      ]
  },
  "case_insensitive": {
    "default": false,
    "description": "Whether to make post-regex comparisons case-insensitive.",
    "type": "boolean"
  },
  "filename": {
    "default": "",
    "description": "Name for output report (optional, defaults to 'transfer-log-report').",
    "type": "string"
  }
}
```

## Output

[//]: # (jj - The below is a little confusing.  Does this only report errors detected? Or does it report everything "resolved" as well?)
### transfer-log-report

This Gear outputs a `csv` or `json` file with the fields: `'error'`, `'path'`,  `'type'`, `'resolved'`, `'label'`, `'_id'` where:

* `path` is the Flywheel resolver path to the container
* `error` describes the type of error encountered for the container
* `type` is the Flywheel container type (i.e. session)
* `resolved` is whether a subsequent gear run detected that the error was resolved
* `label` denotes the label of the Flywheel container
* `row_or_id` is the Flywheel container id for the container or the row of the transfer log

**As of version 1.0.0, the transfer-log-report also includes keys/columns for the Flywheel field values for each container/transfer log row.**

If an item in the transfer log is missing from Flywheel, the `'error'` column will be populated with:
`row <row number> missing from flywheel` the row number is the number that will be displayed when viewing the spreadsheet in a spreadsheet application such as Microsoft Excel (the true row index plus 2, accounting for the header and convention of counting from 1).

[//]: # (jj - Should this bi-directional cross-validation be referenced earlier in this README? It seems important to know that is what this gear is actually doing. Are the "missing from Flywheel" and "Missing from trx log" the only "types" of errors that occur? I don't see any other error types referenced anywhere.)
Conversely, if an item in Flywheel is missing from the transfer log, the `'error'` column will be populated with:
`<container> in flywheel not present in transfer log`

### Flywheel metadata updates

This gear updates the analysis label to `TRANSFER_ERROR_COUNT_<error count>_AT_<timestamp>` upon successful execution.

## Troubleshooting

As with any gear, the Gear Logs are the first place to check when something appears to be amiss. If you are not a site admin, you will not be able to access the Jobs Log page, so do not delete your analysis until you have copied the gear log and downloded the output files. Further, output files will not be available if you delete the analysis.

If you require further assistance from Flywheel, please include a copy of the gear log, the input transfer log, the input template, the output transfer-log-report, and a link to the project/session/subject on which you ran the gear in your correspondence for best results.
