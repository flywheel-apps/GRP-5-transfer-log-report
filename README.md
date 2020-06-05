[![CircleCI](https://circleci.com/gh/flywheel-apps/GRP-5-transfer-log-report.svg?style=svg)](https://circleci.com/gh/flywheel-apps/GRP-5-transfer-log-report)

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

### case_insensitive (default = false)
case_insensitive specifies whether to drop all string values to lowercase when comparing between Flywheel and the transfer
log
### match_containers_once (default = false)
match_containers_once specifies whether to drop `<container> in flywheel not present in transfer log` errors for containers that match at least one transfer log record

### filename (default = "transfer-log-error-report.csv")
filename specifies the name for the output error report file 

### Manifest JSON for configuration options
``` json
"config": {
  "case_insensitive": {
    "default": false,
    "description": "Whether to make post-regex comparisons case-insensitive.",
    "type": "boolean"
  },
  "match_containers_once": {
  "default": false,
  "description": "If true, 'acquisition in flywheel not present in transfer log' errors will not be logged when multiple files from the same container match a transfer log row. (default=false)",
  "type": "boolean"
  },
  "filename": {
    "default": "transfer-log-error-report.csv",
    "description": "Name for output report (optional, defaults to 'transfer-log-report').",
    "type": "string"
  }
}
```

## Output

### transfer-log-report
[Example csv](tests/unit_tests/data/example_error_report.csv) This Gear outputs a csv file with standard columns: `'flywheel_id'`, `'transfer_log_rows'`,  `'error'`, `'matching_fw_ids'`, and `'path'` where:
* `flywheel_id` is the Flywheel container ID belonging to the container (empty for errors originating from transfer
log rows)
* `transfer_log_rows`is a list of Flywheel container IDs that have the same field values as the container
* `error` describes the type of error encountered for the container

In addition to these standard columns, columns for the fields on which records were matched are also included between the
`'transfer_log_rows'` and `'error'` columns.

**As of version 1.0.0, the transfer-log-report also includes keys/columns for the Flywheel field values for each container/transfer log row.**

If a record in the transfer log is missing from Flywheel, the `'error'` column will be populated with:
`row <row number> missing from flywheel` the row number is the number that will be displayed when viewing the spreadsheet in a spreadsheet application such as Microsoft Excel (the true row index plus 2, accounting for the header and convention of counting from 1).

Conversely, if a record in Flywheel is missing from the transfer log, the `'error'` column will be populated with:
`<container> in flywheel not present in transfer log`

If both Flywheel and the transfer logs have matching records, but the number of records for Flywheel and the transfer log differ, the `'error'` column will be populated with:
`<difference> more records in <flywheel or transfer_log> than in <transfer_log or flywheel>`

### Flywheel metadata updates
This gear updates the analysis label to `TRANSFER_ERROR_COUNT_<error count>_AT_<timestamp>` upon successful execution.

## Troubleshooting
As with any gear, the Gear Logs are the first place to check when something appears to be amiss. If you are not a site admin, you will not be able to access the Jobs Log page, so do not delete your analysis until you have copied the gear log and downloded the output files. Further, output files will not be available if you delete the analysis.

If you require further assistance from Flywheel, please include a copy of the gear log, the input transfer log, the input template, the output transfer-log-report, and a link to the project/session/subject on which you ran the gear in your correspondence for best results.
