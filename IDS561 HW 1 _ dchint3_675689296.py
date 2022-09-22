#!/usr/bin/env python
# coding: utf-8

#Chintal Dora Babu
#UIN: 67568926

def data_clean(file):
    '''Funtion to clean the data
    Input
    -file: Input file path as string
    Return:
    cleaned_lines: List of lines'''
    with open(file) as f: # Open file
        lines = f.readlines() #read data file
        #print(f"\n\nNumber of entries: {len(lines)}") #checking how many lines of data are in line
      
     # Cleaning up Data    
    cleaned_lines = [] #list for storing cleaned up data
    for line in lines: # Get list of pairs of data
        line = line.replace(" ", "") #remove whitespace between pairs of data
        line = line.replace("\n", "") #remove new line character
        line = line.replace("(", "") #remove parentheses
        line = line.replace(")", "") #remove parentheses
      
        line = line.split(",") #splitting data by "," to convert data
        
        # Split off month values from year/month 
        for i in range(4):
            if len(line[i]) == 6:  #look at values with 6 characters to differentiate year/month values from max temp values
                year = line[i]
                year = year[:-2] #only take first 4 values of year/month values to just get year
                line[i] = year

        cleaned_lines.append(line) #append updated values to cleaned_lines list

    cleaned_lines = [[int(i) for i in line] for line in cleaned_lines] #change to integer

    return cleaned_lines


def data_split(rawdata, partition=0.5): #split the dataset into 2 parts
    '''Funtion to split the data into parts of given partition size'''
    split =  int(len(rawdata) * partition) #split data by designated partition and convert to integer type

    a, b = rawdata[split:], rawdata[:split] #make the split data into 2 lists
    return a, b #return the 2 lists


def fun_map(split):
    '''Mapper Function - create Key-value pairs of Part1 and Part2'''
    z = []
    for i in range(len(split)):
        a = split[i][0:2] #split by first 2 elements
        b = split[i][2:4] #split by last 2 elements
        z.append(a) #append to list
        z.append(b) #append to list
    return z


def sort_data(data):
    '''Sort Function - Sorted Key-value pairs for the whole dataset'''
    data = sorted(data) #sorts by key-value pairs
    return data


def partition(sort):
    '''#Partition Function -Two ascending ordered partitions'''
    #declaring the reducer lists
    rdc1 = []
    rdc2 = []
    #Looping through the keys of each sublist and splitting based on date range, one for years 2010-2015 and the other 2016-2020
    for i in range(len(sort)):
        if sort[i][0] >= 2010 and sort[i][0] <= 2015:
            rdc1.append(sort[i])
        else:
            rdc2.append(sort[i])
    return rdc1,rdc2


def reducer(reducer_data):
    '''Reducer Function - Maximum temperature of the ordered partitions'''
    unique_keys = dict([]) #keep track of the years/unique keys

    # Group by Key 
    for pair in reducer_data:
        key = pair[0]
        val = pair[1]
        if key not in unique_keys:
            unique_keys[key] = []

        unique_keys[key].append(val)
    
    # Get Max Temp from dictionary
    for key in unique_keys:
        max_temp = max(unique_keys[key])
        unique_keys[key] = max_temp
    
    return unique_keys


def main():
    '''Main funtion'''
    import csv #to write output to csv
    import pandas as pd

    # Read Data File / Data Cleaning
    f = "temperatures.txt"
    a = data_clean(f)

    # Split Data
    b = data_split(a)
    
    #Mapper
    A = fun_map(b[0])
    B = fun_map(b[1])
    
    # Sort Data 
    sorted_A = sort_data(A)
    sorted_B = sort_data(B)

    # Partition Data
    p = partition(sorted_A)
    q = partition(sorted_B)

    # Reducer
    r = p[0]+p[1]+q[0]+q[1]
    R = reducer(r)
    

    Final_result = R
    #Convert results into Pandas Dataframe
    df = pd.DataFrame(list(Final_result.items()),columns = ['Year','Max Temp']) 

    #Final output into CSV File that can be found in the content folder
    df.to_csv ('maxtempyearlyoutput.csv', index = False, header=True)

if __name__ == '__main__':
	main()





