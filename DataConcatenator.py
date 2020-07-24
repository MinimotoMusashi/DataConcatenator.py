from chardet.universaldetector import UniversalDetector
from collections import defaultdict
import pandas as pd
import chardet
import glob
import os

def main_loop():
    print('Main loop has started...')
    scan_list = scan_for_files_in_cwd()
    encode_list = get_list_of_files_to_encode(scan_list)
    encode_utf8_files_to_utf16(encode_list)
    dict_grouped_by_rig_numbers = group_files_by_rig_number(scan_list)
    concatenate_files_by_rig_group(dict_grouped_by_rig_numbers)
    print('Done.')
    
def scan_for_files_in_cwd():
    print('Scanning for files...')
    file_list = glob.glob('./*_*.csv')
    return file_list

def get_list_of_files_to_encode(scan_list):
    print('Creating list of files that need to be encoded...')
    encode_list = []
    for i in scan_list:
        detector = UniversalDetector()
        detector.reset()
        with open(i, 'rb') as input_file:
            for row in input_file:
                detector.feed(row)
                if detector.done: break
            result = detector.result
            detector.close()
            if result['encoding'] == None: 
                encode_list.append(i)
    return encode_list

def encode_utf8_files_to_utf16(encode_list):
    print('Encoding the files...')
    for i in encode_list:
        df = pd.read_csv(i, encoding='UTF-8')
        df.to_csv(i, encoding='UTF-16')

def group_files_by_rig_number(scan_list):
    print('Grouping files by rig number...')
    groups = defaultdict(list)
    for i in scan_list:
        basename, extension = os.path.splitext(i)
        rig_number = basename.split('_')[0].split('\\')[1]
        groups[rig_number].append(i)
    return groups

def concatenate_files_by_rig_group(dict_grouped_by_rig_numbers):
    file_name =  'July Nabors controls Data 0701-0721'
    print('Concatenating the files into one complete file for each rig...')
    for key, val in dict_grouped_by_rig_numbers.items():
        temp_list = []
        for i in val:
            df = pd.read_csv(i, encoding='UTF-16')
            df['Time'] = pd.to_datetime(df['Time'])
            temp_list.append(df)
        concatenated_df = pd.concat(temp_list, ignore_index=True, sort=False)
        concatenated_df.sort_values(by=['Time'], inplace=True)
        concatenated_df.to_csv(key + ' ' + file_name + '.csv', encoding='UTF-16', index=False)
        print('Finished concatenating rig: ' + key + ', Number of files: ' + str(len(val)))

main_loop()
