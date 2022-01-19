import pandas as pd
import os
import json
import sys
from nestshredder.pyshred_core import _shred_recursive

def shred_json(filepath,targetpath,objname):

    if os.path.exists(targetpath) == False:
        is_success = 'Target Path Does Not Exist'
        print("1:Error - " + is_success )
        return "1:Error - " + is_success   

    if os.path.exists(filepath) == False:
        is_success = 'Source Path Does Not Exist'
        print("1:Error - " + is_success )
        return "1:Error - " + is_success   

    try:
        json_df = pd.read_json(filepath)
        is_success = _shred_recursive(json_df,targetpath,objname,objname,objname)
    except Exception as e:        
        if str(e) == 'If using all scalar values, you must pass an index':
            new_list = []
            with open(filepath) as json_file:
                data = json.load(json_file)
                new_list.append(data)
                json_df = pd.DataFrame.from_dict(new_list)
                is_success = _shred_recursive(json_df,targetpath,objname,objname,objname)
        else:
            is_success = str(e)

    if is_success != "0":
        print("1:Error - " + is_success )
        return "1:Error - " + is_success        
    else: 
        return "0:OK"

def shred_parquet(filepath,targetpath,objname):

    if os.path.exists(targetpath) == False:
        is_success = 'Target Path Does Not Exist'
        print("1:Error - " + is_success )
        return "1:Error - " + is_success   

    if os.path.exists(filepath) == False:
        is_success = 'Source Path Does Not Exist'
        print("1:Error - " + is_success )
        return "1:Error - " + is_success   

    try:
        json_df = pd.read_parquet(filepath)
        print(json_df)
        is_success = _shred_recursive(json_df,targetpath,objname,objname,objname)
    except Exception as e:        
        is_success = str(e)

    if is_success != "0":
        print("1:Error - " + is_success )
        return "1:Error - " + is_success        
    else: 
        return "0:OK"
