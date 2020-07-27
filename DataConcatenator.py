from chardet.universaldetector import UniversalDetector
from collections import defaultdict
import pandas as pd
import glob
import os
import time

'''
Data Concatenator.
Author: William Bailey Brumble
Purpose: Concatenate data pulled at weekly intervals into one file.
Date: 07/27/2020
'''

# True = Controls, False = ECM
controls_or_ecm = True

def main_loop():
    global start
    print('Main loop has started...')
    start = time.time()
    scan_list = scan_for_files_in_cwd()
    encode_list = get_list_of_files_to_encode(scan_list)
    encode_utf8_files_to_utf16(encode_list)
    dict_grouped_by_rig_numbers = group_files_by_rig_number(scan_list)
    concatenate_files_by_rig_group(dict_grouped_by_rig_numbers)
    end = time.time()
    print('Done. Time elapsed: ' + str(end - start))
    
def scan_for_files_in_cwd():
    print('Scanning for files...')
    file_list = glob.glob('./*_*.csv')
    end = time.time()
    print('Done. Time elapsed: ' + str(end - start))
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
    end = time.time()
    print('Done. Time elapsed: ' + str(end - start))
    return encode_list

def encode_utf8_files_to_utf16(encode_list):
    print('Encoding the files...')
    for i in encode_list:
        df = pd.read_csv(i, encoding='UTF-8')
        df.to_csv(i, encoding='UTF-16', index=False)
        end = time.time()
        print('Finished encoding file: ' + i + ', elapsed time: ' + str(end - start))

def group_files_by_rig_number(scan_list):
    print('Grouping files by rig number...')
    groups = defaultdict(list)
    for i in scan_list:
        basename, extension = os.path.splitext(i)
        rig_number = basename.split('_')[0].split('\\')[1]
        groups[rig_number].append(i)
    end = time.time()
    print('Done. Time elapsed: ' + str(end - start))
    return groups

def concatenate_files_by_rig_group(dict_grouped_by_rig_numbers):
    if controls_or_ecm == True:
        file_name =  'July Nabors controls Data 0701-0721'
    else:
        file_name =  'July ECM Data 0701-0721'
    print('Concatenating the files into one complete file for each rig...')
    for key, val in dict_grouped_by_rig_numbers.items():
        temp_list = []
        print('Starting concatenating rig: ' + key + ', Number of files: ' + str(len(val)))
        for i in val:
            df = pd.read_csv(i, encoding='UTF-16')
            if controls_or_ecm == True:
                df['Time'] = pd.to_datetime(df['Time'])
            else:
                df['Date'] = pd.to_datetime(df['Date'])
            temp_list.append(df)
        concatenated_df = pd.concat(temp_list, ignore_index=True, sort=False)
        if controls_or_ecm == True:
            concatenated_df.sort_values(by=['Time'], inplace=True)
        else:
            concatenated_df.sort_values(by=['Date'], inplace=True)
        concatenated_df.to_csv(key + ' ' + file_name + '.csv', encoding='UTF-16', index=False)
        end = time.time()
        print('Finished concatenating rig: ' + key + ', elapsed time: ' + str(end - start))

main_loop()
