# Apache Beam Clevertap Stream Data Using Apache Beam Example

### Description: 
This repo is intended for transferring Clevertap data from GCS to BQ via straming API with DataFlow runner. Before you can dump your data, you have to create a table with the following schema: 
```JSON
[
  {
    "column_name": "ts",
    "field_path": "ts",
    "data_type": "TIMESTAMP",
    "description": null
  },
  {
    "column_name": "event_name",
    "field_path": "event_name",
    "data_type": "STRING",
    "description": null
  },
  {
    "column_name": "event_props",
    "field_path": "event_props",
    "data_type": "STRING",
    "description": null
  },
  {
    "column_name": "device_info",
    "field_path": "device_info",
    "data_type": "STRING",
    "description": null
  },
  {
    "column_name": "profile",
    "field_path": "profile",
    "data_type": "STRING",
    "description": null
  },
  {
    "column_name": "session_props",
    "field_path": "session_props",
    "data_type": "STRING",
    "description": null
  }
]
```

Requirements:
- Python version 3.9

Notes:
- Client API will get the credentials string from GOOGLE_APPLICATION_CREDENTIALS environment variable