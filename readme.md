# Python Nest Shredder
## Turn nested data into relational files!

Nest Shredder is a pandas-wrapper utility for converting nested JSON data into relational Parquets, typically for onward consumption into a relational database (where nested data may become less immediately useful).

## Features

- Give the tool a JSON or Parquet and a target output path
- ...And you'll get a bunch of Parquet in the output path!
- Names the parquet files based on the path of the nested data
- Adds some id columns for relational integrity from source objects

## Doesn't Feature(s)

- If you shred the same JSON object twice that has a nested array of objects I don't guarantee the order for each shred-time (but the ids will be valid for the run). Get yourself a key on that object! :) 
- No CSV at the moment. Simple add, will do later.
- No compression on the Parquet as standard. Will add later.

## Tech

Nest Shredder uses a couple of open source projects to work properly:

- [Pandas] - For it's lovely dataframes.
- [Pyarrow] - To generate the Parquet files.

## Installation

PyPi via PipEnv or Pip itself. Up to you!

## License

MIT

[//]: # 

   [pandas]: <https://github.com/pandas-dev/pandas>
   [pyarrow]: <https://github.com/apache/arrow/tree/master/python/pyarrow>
