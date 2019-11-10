# flywheel/GRP-5-transfer-log-report

Validate existence of data referenced within a transfer report log within a given project. This Gear outputs a csv file with the fields: 'error', 'path',  'type', 'resolved', 'label', '_id'.

## INPUTS
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

## Configuration

See examples/transfer-log-template.yml

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
This Gear outputs a `csv` or `json` file with the fields: `'error'`, `'path'`,  `'type'`, `'resolved'`, `'label'`, `'_id'`.

## TO DO
- [ ] describe config file options
