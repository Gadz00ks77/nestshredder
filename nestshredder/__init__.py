import pandas as pd
import logging
import json
from nestshredder.pyshred_core import _shred_recursive, pad_dict_list
from nestshredder.pyshred_util import check_arguments

def shred_json(source_file_path,target_folder_path,object_name,batch_ref=None):

    check_arguments(source_file_path,target_folder_path,object_name,batch_ref)

    try:
        json_df = pd.read_json(source_file_path)
        shred_outcome = _shred_recursive(json_df,target_folder_path,object_name,object_name,object_name,batch_ref)
    except Exception as e:        
        if str(e) == 'If using all scalar values, you must pass an index':
            new_list = []
            with open(source_file_path) as json_file:
                data = json.load(json_file)
                new_list.append(data)
                json_df = pd.DataFrame.from_dict(new_list)
                shred_outcome = _shred_recursive(json_df,target_folder_path,object_name,object_name,object_name,batch_ref)
        elif str(e) == 'All arrays must be of the same length':  
            new_list = []
            with open(source_file_path) as json_file:
                data = json.load(json_file)
                new_list.append(data)
                padded_list = pad_dict_list(new_list,'n/a')
                json_df = pd.DataFrame.from_dict(padded_list)
                shred_outcome = _shred_recursive(json_df,target_folder_path,object_name,object_name,object_name,batch_ref)
        else:
            shred_outcome = str(e)
    if shred_outcome != "0":
        logging.error(shred_outcome)
        return        
    else: 
        logging.info(f"Completed processing for object name: {object_name} at source file path: {source_file_path}")
        return

def shred_parquet(source_file_path,target_folder_path,object_name,batch_ref=None):

    check_arguments(source_file_path,target_folder_path,object_name,batch_ref)

    try:
        json_df = pd.read_parquet(source_file_path)
        shred_outcome = _shred_recursive(json_df,target_folder_path,object_name,object_name,object_name,batch_ref)
    except Exception as e:        
        logging.error(str(e))
        return

    if shred_outcome != "0":
        logging.error(shred_outcome)
        return        
    else: 
        logging.info(f"Completed processing for object name: {object_name} at source file path: {source_file_path}")
        return
