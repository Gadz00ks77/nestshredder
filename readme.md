# Python Nest Shredder
## Turn nested data into relational files!

Nest Shredder is a pandas-wrapper utility for converting nested JSON or Parquet data into relational "flat" Parquet files, typically for onward consumption into a relational database (where nested data may become less immediately useful).

## Features

- Give the tool a nested JSON or Parquet and a target output path
- ...And you'll get a bunch of "flat" Parquet in the output path!
- Names the parquet files based on the path of the nested data
- Adds some id columns for relational integrity from source objects
- Shred functions accept a batch identifier for output metadata.
- Supports standard path or file-like inputs of Pandas for read_json / read_parquet methods.
- Defaults to Parquet output type but will output to JSON / CSV (no compression supported on these latter two at the moment).

## Will Soon Feature
- No compression on the output Parquet as standard. Will add later.
- A way to push each result file Parquet back to an S3 bucket or similar. Useful. Will add later.
- Expose the config options for CSV generation. May be.
- Support for other Parquet libraries. May be later.
- Represent the full path in the parquet output to account for people naming child objects the same thing repeatedly. Will add later and burst into tears. Model your data properly.


## Doesn't Feature(s)

- If you shred the same JSON object twice that has a nested array of objects it doesn't guarantee the order for each shred-time (but the ids will be valid for the run). Get yourself a key on that object! :) 



## Tech

Nest Shredder uses a couple of open source projects to work properly:

- [Pandas] - For it's lovely dataframes.
- [Pyarrow] - To generate the Parquet files.

## Installation

PyPi via PipEnv or Pip itself. Up to you!

## Simple Usage

The module exposes two functions at the moment;
1. shred_json
2. shred_parquet

Both accept:
- path_or_buff *- the source file path (e.g. './examples/test.json') or a BytesIO-like file object*
- target_folder_path *- the path where you would like your flattened / unnested outputs. New folders will be created in here, using:*
- object_name *- a simple string that you can use to identify the overall object represented by your data (e.g. Customers or Addresses). One word only please.*

e.g.
```python
import nestshredder as ns

ns.shred_json('./examples/vsimple_example.json','./target','example')
```

- added a batch_ref identifier to further describe the object you're shredding.

e.g.
```python
import nestshredder as ns

ns.shred_json('./examples/vsimple_example.json','./target','example','ABC123')
```

- shred_json also exposes most of the read_json Pandas stuff too in case you need it.


## Other Usage Nodes (S3 etc)

As with Pandas - you can read a file from S3 and pass it to one of the shred functions as a Streaming object. e.g.

```python
import nestshredder as ns
import boto3

s3 = boto3.client('s3')
bucket='marvellous-bucket-name-here'
data = s3.get_object(Bucket=bucket, Key='s3foldername/file.json')
contents = data['Body']
ns.shred_json( path_or_buf=contents,
               target_folder_path='./targets',
               object_name='objname',
               batch_ref='ABCD1246325'
               )
```

## Known Limitations
At the moment this uses a recursion - which is not wonderful. I'll work to refactor over the coming ... days? 

## License

MIT

[//]: # 

   [pandas]: <https://github.com/pandas-dev/pandas>
   [pyarrow]: <https://github.com/apache/arrow/tree/master/python/pyarrow>
