# GCS File Processing

This Python script processes files from a local directory, uploads them to Google Cloud Storage (GCS), and reads them back from GCS.

## Setup

1. Install required Python packages using pip:
``` shell
pip install -r requirements.txt

```
## Set up Google Cloud Storage:

### Create a GCS bucket named pythongcsde.
* Configure authentication for the Google Cloud SDK or set up a service account key file for authentication.
* Update the script with your source directory and schema file paths:

* src_directory: Path to the directory containing the files to be processed.
* schema_file: Path to the JSON schema file containing Parquet schema definitions.
## Usage
Run the script using Python:
``` shell
python process_files.py
```
The script will process each file in the specified directory, upload it to GCS, and read it back as a DataFrame.