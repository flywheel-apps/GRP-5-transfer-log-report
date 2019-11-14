# flywheel/GRP-5-transfer-log-report

GRP-5 is a Flywheel analysis gear to validate existence of data referenced within a transfer report log within a given project. This Gear outputs a csv file with the fields: 'error', 'path',  'type', 'resolved', 'label', '_id'.

## INPUTS

### transfer_log (required)
The transfer_log is a csv (or xls) file that describes the records that should be present in Flywheel
### template
The template is a yaml file that describes how to map the transfer log to objects in Flywheel
See examples/transfer-log.xlsx and transfer-log-template.yml for a transfer log and transfer log template example.
### Manifest JSON for Inputs
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
```
query:
  - subject.label: "Subject"
  - session.label: "Timepoint"
 ```
If a subject.label in Flywheel and the transfer log Subject value were both "subject1", but session.label and "Timepoint" were week4 and Week4, respectively, they would not be considered matches unless case_insensitive was set to `true`.

### file_type (default = csv)
file_type specifies the format to which to save the output error report file (csv or json).

### filename (default = "transfer-log-report")
filename specifies the name for the output error report file 

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

### transfer-log-report
This Gear outputs a `csv` or `json` file with the fields: `'error'`, `'path'`,  `'type'`, `'resolved'`, `'label'`, `'_id'` where:
* `path` is the Flywheel resolver path to the container
* `error` describes the type of error encountered for the container
* `type` is the Flywheel container type (i.e. session)
* `resolved` is whether a subsequent gear run detected that the error was resolved
* `label` denotes the label of the Flywheel container
* `_id` is the Flywheel container id for the container

If an item in the transfer log is missing from Flywheel, the `'error'` column will be populated with:
`row <row number> missing from flywheel` the row number is the number that will be displayed when viewing the spreadsheet in a spreadsheet application such as Microsoft Excel (the true row index plus 2, accounting for the header and convention of counting from 1).

Conversely, if an item in Flywheel is missing from the transfer log, the `'error'` column will be populated with:
`<container> in flywheel not present in transfer log`

### Flywheel metadata updates
This gear updates the analysis label to `TRANSFER_ERROR_COUNT_<error count>_AT_<timestamp>` upon successful execution.

## Troubleshooting
As with any gear, the Gear Logs are the first place to check when something appears to be amiss. If you are not a site admin, you will not be able to access the Jobs Log page, so do not delete your analysis until you have copied the gear log and downloded the output files. Further, output files will not be available if you delete the analysis.

Two log statements that are specifically useful are `DEBUG:root:transfer_log_values:dict_keys` and `DEBUG:root:flywheel_values: dict_keys`. If you believe that a row in your transfer log should match a Flywheel container, then comparing the values here will often explain why the two were not considered a match. Each item wrapped in parentheses describes a single Flywheel container or transfer log row. The last item in a transfer log row will always be None because this is reserved for the container id which a row in a file cannot have.

If you require further assistance from Flywheel, please include a copy of the gear log, the input transfer log, the input template, the output transfer-log-report, and a link to the project/session/subject on which you ran the gear in your correspondence for best results.
