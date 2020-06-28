# Benchmaster

Benchmaster is a small SoftIron python utility which aims to make the testing and reporting workflow of our benchmarking much less painful.

It currently supports:

- Creating and writing to Google Spreadsheets
- Creating new RGW/S3 Users
- Checking S3 functionality with boto3
- Generating and running Cosbench workloads with S3 or Librados

# Getting Started

Running on a new machine:

```bash
apt-get install python3-pip
pip3 install boto gspread docopt

./benchmaster.py --help
```
