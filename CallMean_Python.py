#CallMean Python Version
import os
import numpy as np
import matplotlib.pyplot as plt
from configobj import ConfigObj
import fstpy.all as fstpy
from fstpy.std_io import get_basic_dataframe, add_dask_column
from fstpy.dataframe import add_grid_column
import pandas as pd
import dask.array as da
import glob
import shutil
pd.options.mode.chained_assignment = None  # default='warn'

#Copy and paste . ssmuse-sh -d /fs/ssm/eccc/cmd/cmde/surge/surgepy/1.0.8/ in interpreter path when you run program through IDE

def main():
    config = ConfigObj(os.path.join(os.getcwd(), 'CallMean_config'))
    section1 = config['Main Variables']
    
    #Variables are defined in config file
    directory = section1['directory']
    save_path = section1['save_path']
    
    #Create new directory for data to be saved to
    os.makedirs(save_path, exist_ok = True)
    
    #MatPlotGraphics will be saved to current directory
    graphics_save_path = save_path + '_MatPlotGraphics'
    os.makedirs(graphics_save_path, exist_ok = True)
    
    start_time = int(section1['start_time'])
    increment = int(section1['increment'])
    end_time = int(section1['end_time'])
    start_date = section1['start_date']
    end_date = section1['end_date']
    double_count = 0
        
    include_runs = section1['include_runs']
    include_runs = sorted(map(int,  include_runs))
  
    skip_corrupted_files = section1['skip_corrupted_files']
    if(type(skip_corrupted_files) != list):
        skip_corrupted_files = [skip_corrupted_files]
    #the ip1 values will change based on directory. You will have to find
    #what the ip1 value is to ensure you are only averaging the surface level
    section2 = config['ip1 variables']
    keys = section2['vars']
    values = section2['ip1']
    values2 = section2['average']
    values3 = section2['is_vector']
    
    #Special Case if only 1 variable is given by user
    if((type(keys) != list) and (keys != '')):
        keys = [keys]
    if((type(values) != list) and (values != '')):
        values = [values]
    if((type(values2) != list) and (values2 != '')):
        values2 = [values2]
    if((type(values3) != list) and (values3 != '')):
        values3 = [values3]
    
    ip1_dict = dict(zip(keys, zip(values, values2, values3)))
    print(ip1_dict)
    #===================================
    #Error handling
    
    #Check if end_time is reachable given increment
    valid_runs = []
    error = start_time
    for i in range(start_time,  end_time+1):
        valid_runs.append(error)
        error = error + increment
        if(error > end_time):
            raise ValueError('Incrementing the start time will not result in the end time')
        elif(error == end_time):
            valid_runs.append(error)
            break
    
    if(end_time < start_time):
        raise Exception("End time is less than start time")
    
    #Check ip1 variables are same length
    if all(len(lst) == len(keys) for lst in [values, values2, values3]):
        if(len(keys) == 0):
            raise Exception("Length of one of the lists in ip1 variables is zero")
        print("All lists are same length")
    else:
        raise Exception("Vars, ip1, average, and is_vector are not the same length")
    
    if((int(start_date[-2:]) not in include_runs) or (int(end_date[-2:]) not in include_runs)):
        raise Exception("Start and end date will never be reached due to include_runs")
    
    if((type(include_runs) != list) and (include_runs != '')):
        include_runs = [include_runs]
    elif (len(include_runs) == 0):
        raise Exception("Nothing written in include_runs")
    
    if(len(include_runs) == 1):
        if(end_time - start_time + 1 != 24):
            raise Exception("Start time, end time, and include_runs do not cover a 24 hour period")
    else:
        double_count = int(include_runs[1]) - int(include_runs[0]) 
        if((double_count) * (len(include_runs)) != 24):
            #Likely broken
            raise Exception("Include_runs does not cover a 24 hour period. May result in double counting")
        elif(len(valid_runs) * len(include_runs) * increment!= 24):
            raise Exception("Include_runs does not cover a 24 hour period. May result in double counting")
    
    
    #===================================
    
    #Build directory - can be used for MSHydro directory
    section3 = config['Build temp directory']
    if section3['enable'] == 'y':
        temp_save_path = section3['temp_directory_save_path']
        source = section3['directory']
        os.makedirs(temp_save_path, exist_ok = True)
        temp_directory = glob.glob(source)
        
        start_counter = 0
        print("\n Copying files to directory")
        for files in sorted(temp_directory):
            
            if((files[-14:-4] != start_date) and (start_counter == 0)):
                continue
            elif(files[-14:-4] == start_date):
                start_counter = 1
            if(files[-2:].isalpha() == True):
                continue
            
            if( (int(files[-3:]) < start_time  - 1) or (int(files[-3:]) > end_time) ):
                continue
                
            if files[-14:-4].isnumeric():
                shutil.copy(files, temp_save_path)
                
            if(files[-14:-4] == end_date and int(files[-3:]) == end_time):
                break
        directory = temp_save_path
        print("New directory is, " + directory)
    
    CallMean(directory,save_path, graphics_save_path, start_time, increment, end_time, start_date, end_date, include_runs,skip_corrupted_files,  ip1_dict)
    
def CallMean(directory,save_path,graphics_save_path, start_time, increment, end_time, start_date, end_date, include_runs, skip_corrupted_files, ip1_dict):
    print('\n', "Starting....")
    print("Analyzing directory: ",  directory)
    atm_variables =  [] #Complete list
    atm_variables1 = [] #TT, HU, FB, FI, PN - average
    atm_variables2_temp = []
    atm_variables2 = [] #UU, VV - vectors
    atm_variables3 = [] #PR - Summation

    #Split ip1_dict into seperate lists based on average, vector, or summation variable
    for key, value in list(ip1_dict.items()):
        atm_variables.append(key)
        if(value[2] != 'n'):
            atm_variables2.append(key)
            atm_variables2.append(value[2])
            atm_variables2_temp.append(value[2])
            ip1_dict[str(value[2])] = (ip1_dict.get(key)[0], 'vector',  'n')
            continue
        if(value[1] == 'y'):
            atm_variables1.append(key)
            continue
        if(value[1] == 'n'):
            atm_variables3.append(key)
            continue
    
    for i in atm_variables2_temp:
        atm_variables.append(i)
    
    atm_variables.append(">>")
    atm_variables.append("^^")
    atm_variables.append("^>")
    
    total_processed = 0
    daily_processed = 0
    start_counter = 0
    new_date = start_date
    
    all_dfs = []
    sum_start_time_df = []
    sum_end_time_df = []
    #Start looping through files
    for fileName in sorted(os.listdir(directory)):
        #Skip all files until we reach our start date
        if((fileName[0:10] != start_date) and (start_counter == 0)):
            continue
        elif((fileName[0:10] == start_date) and (start_counter == 0)):
            start_counter = 1
        
        
        if(new_date != fileName[0:10]):
            #For increment purposes
            daily_processed = 0
            new_date = fileName[0:10]
        
        if(int(fileName[8:10]) not in include_runs):
            continue
        
        if(fileName[-2:].isalpha() ==True):
            #Skip files with extension
            continue
        elif((int(fileName[-3:]) < start_time - 1) or (int(fileName[-3:]) > end_time)): 
            #Skip any files outside of our start and end time. We keep start_time - 1 for summation variables
            continue
        
        
        the_file = os.path.join(directory,  fileName)
        if(fileName in skip_corrupted_files):
            print("Skipping corrupted file: ",  fileName)
            continue
        
        #Save start time - 1 and end time files to seperate dataframes for summation variables
        if(int(fileName[-3:]) == (start_time - 1)):
            try:
                df = get_basic_dataframe(the_file)
            except:
                raise Exception(fileName,  'possibly corrupted')
            df = df.loc[df.nomvar.isin(atm_variables3)]
            
            for var in atm_variables3:
                df = df.drop(df[(df.nomvar == var)&(df.ip1!=int(ip1_dict.get(var)[0]))].index)
            
            sum_start_time_df.append(df)
            total_processed += 1
            continue
        elif(int(fileName[-3:]) == 0):
            try:
                df = get_basic_dataframe(the_file)
            except:
                raise Exception(fileName,  'possibly corrupted')
            df = df.loc[df.nomvar.isin(atm_variables3)]
            
            for var in atm_variables3:
                df = df.drop(df[(df.nomvar == var)&(df.ip1!=int(ip1_dict.get(var)[0]))].index)
            
            sum_start_time_df.append(df)
        elif(int(fileName[-3:]) == end_time):
            try:
                df = get_basic_dataframe(the_file)
            except:
                raise Exception(fileName,  'possibly corrupted')
            df = df.loc[df.nomvar.isin(atm_variables3)]
            
            for var in atm_variables3:
                df = df.drop(df[(df.nomvar == var)&(df.ip1!=int(ip1_dict.get(var)[0]))].index)
            
            sum_end_time_df.append(df)
        
        #Skip files based on increment
        if(daily_processed % increment != 0):
            daily_processed += 1
            continue
        
        #Save all variables to one large dataframe
        try:
            df = get_basic_dataframe(the_file)
        except:
            raise Exception(fileName,  'possibly corrupted')
            
        df = df.loc[df.nomvar.isin(atm_variables)]
        for var in atm_variables:
            if((var == '>>') or (var == '^^') or (var == '^>')):
                continue
            df = df.drop(df[(df.nomvar == var)&(df.ip1!=int(ip1_dict.get(var)[0]))].index)
        #print(df['nomvar'])
        all_dfs.append(df)
        total_processed += 1
        daily_processed += 1
        
        if((fileName[0:10] == end_date) and (int(fileName[-3:]) == end_time)):
            #Once we reach the defined end date and end hour, stop looping through files
            break
    
    df = pd.concat(all_dfs, ignore_index=True)
    df = add_grid_column(df)
    cols = list(df.columns)
    if atm_variables3:
        sum_start_df = pd.concat(sum_start_time_df,ignore_index=True)
        sum_start_df = add_grid_column(sum_start_df)
        sum_end_df = pd.concat(sum_end_time_df, ignore_index=True)
        sum_end_df = add_grid_column(sum_end_df)
    
    print("Finished files")
    
    #Save positional data (>>, ^^) to dataframe
    meta_df = df.loc[df.nomvar.isin(['>>','^^', '^>'])]
    meta_df = add_dask_column(meta_df)
    
    #Split the nomvar's up
    df1 = df.loc[df.nomvar.isin(atm_variables1)] # ["TT", "HU", "FB","FI","PN"] - averages
    df2 = df.loc[df.nomvar.isin(atm_variables2)] # ["UU","VV"] - vectors
    
    # Calculate averages for TT, HU, FB, FI, and PN, etc
    if atm_variables1:
        groups = df1.groupby(['grid','nomvar'])
        averages = []
        double_count_vars = []
        for (grid,nomvar), current_var_df in groups:
            print(nomvar,  'done')
            #In lam/nat.eta directory, I realized that it may run each variable twice due to the grid. I only want one result, so I'll ignore the other one
            if(nomvar in double_count_vars):
                continue
            else:
                double_count_vars.append(nomvar)
                
            current_var_df = add_dask_column(current_var_df)
            mean_of_var_df = pd.DataFrame([current_var_df.iloc[0].to_dict()]).reset_index(drop=True)
            arr = da.stack(current_var_df.d)
            mean_of_var_df.at[0,'d'] = np.mean(arr, axis=0)
            averages.append(mean_of_var_df)
        #DataFrame that contains the averages of TT, HU, FB, FI, and PN
        averages_df = pd.concat(averages,ignore_index=True) 
    print("average finish")

    # Calculate summation variables
    if atm_variables3:
        final_sums = []
        for var in atm_variables3:
            start_date_df = sum_start_df.loc[sum_start_df.nomvar == var]
            end_date_df = sum_end_df.loc[sum_end_df.nomvar == var]
            start_df = add_dask_column(start_date_df)
            end_df = add_dask_column(end_date_df)
            summation_df = pd.DataFrame([start_df.iloc[0].to_dict()]).reset_index(drop=True)
            start_arr = da.stack(start_df.d)
            end_arr = da.stack(end_df.d)
            start_avg_arr = np.mean(start_arr,  axis=0)
            end_avg_arr = np.mean(end_arr, axis=0)
            summation_df.at[0,'d'] = np.subtract(end_avg_arr, start_avg_arr)
            final_sums.append(summation_df)
        #DataFrame that contains sums
        sums_df = pd.concat(final_sums,ignore_index=True)
        
    print("Sums finished")
    
    # Calculate vectors
    if atm_variables2:
        df2 = fstpy.add_columns(df2, ['ip_info'])
        groups = df2.groupby(['grid'])
        vector1_averages = []
        vector2_averages = []
        hypot = []
        hypot_index = 0
        for i in range(0, len(atm_variables2), 2):
            for grid, current_df in groups:
                # Calculate vector1 average
                v1_df = current_df.loc[(current_df.nomvar==atm_variables2[i]) & (current_df.surface == True)]
                v1_df = add_dask_column(v1_df)
                if not v1_df.empty:
                    vector1_df = pd.DataFrame([v1_df.iloc[0].to_dict()]).reset_index(drop=True)
                    vector1_df['nomvar'] = atm_variables2[i]
                    arr = da.stack(v1_df['d'])
                    vector1_df.at[0,'d'] = np.mean(arr, axis=0)
                    #print(vector1_df.iloc[0].d.compute())
                    vector1_averages.append(vector1_df)
                print(atm_variables2[i],  'done')
                #Calculate vector2 average
                v2_df = current_df.loc[(current_df.nomvar==atm_variables2[i+1]) & (current_df.surface == True)]
                v2_df = add_dask_column(v2_df)
                if not v2_df.empty:
                    vector2_df = pd.DataFrame([v2_df.iloc[0].to_dict()]).reset_index(drop=True)
                    vector2_df['nomvar'] = atm_variables2[i+1]
                    arr = da.stack(v2_df['d'])
                    vector2_df.at[0,'d'] = np.mean(arr, axis=0)
                    #print(vector2_df.iloc[0].d.compute())
                    vector2_averages.append(vector2_df)
                print(atm_variables2[i+1],  'done')
                #These two will only be used for calculating hypot
                vector1_surface_df = pd.concat(vector1_averages,ignore_index=True)
                vector2_surface_df = pd.concat(vector2_averages,ignore_index=True)
                atm_variables.append(atm_variables2[i][0] + atm_variables2[i+1][0])
                print(atm_variables2[i][0] + atm_variables2[i+1][0],  'done')
                #In lam/nat.eta directory, I realized that it may run each variable twice due to the grid. I only want one result, so I'll ignore the other one
                break
                # Calculate hypotenuse of vector1 and vector2 data points
#                if (not vector1_surface_df.empty) and (not vector2_surface_df.empty):
#                    hypot_surface_average_df = pd.DataFrame([vector1_surface_df.iloc[0].to_dict()]).reset_index(drop=True)
#                    hypot_surface_average_df['nomvar'] = atm_variables2[i][0] + atm_variables2[i+1][0]
#                    atm_variables.append(atm_variables2[i][0] + atm_variables2[i+1][0]) 
#                    v1 = vector1_surface_df.iloc[hypot_index].d
#                    v2 = vector2_surface_df.iloc[hypot_index].d
#                    hypot_data = np.hypot(v1, v2)
#                    hypot_surface_average_df['d'] = [hypot_data]
#                    hypot.append(hypot_surface_average_df)
#                    hypot_index += 1
        #Save vectors to seperate DataFrames
        #vector1_averages_df = pd.concat(vector1_averages, ignore_index=True)
        #vector2_averages_df = pd.concat(vector2_averages, ignore_index=True)
        vector1_averages_df = vector1_surface_df
        vector2_averages_df = vector2_surface_df
        #hypot_df = pd.concat(hypot, ignore_index=True)
    print("Vectors finished")
    def do_nothing():
        pass
    
    #Combine all the DataFrame's previously made into one called combined_df
    #Combined_df will later be used to write seperate RPN files for each nomvar variable
    
    #Edit combined_df in case certain variables were not inputted by user
    #atm_variable1 is average, atm_variable2 is vector, atm_variable3 is summation
    if(not atm_variables1):
        if(not atm_variables2):
            combined_df = pd.concat([sums_df, meta_df], ignore_index=True)
        elif(not atm_variables3):
            combined_df = pd.concat([vector1_averages_df,vector2_averages_df,meta_df], ignore_index=True)
        else:
            combined_df = pd.concat([sums_df,vector1_averages_df,vector2_averages_df,meta_df], ignore_index=True)
    elif(not atm_variables2):
        if(not atm_variables3):
            combined_df = pd.concat([averages_df,meta_df], ignore_index=True)
        else:
            combined_df = pd.concat([averages_df,sums_df,meta_df], ignore_index=True)
    elif(not atm_variables3):
        combined_df = pd.concat([averages_df,vector1_averages_df,vector2_averages_df,meta_df], ignore_index=True)
    else:
        combined_df = pd.concat([averages_df,sums_df,vector1_averages_df,vector2_averages_df,meta_df], ignore_index=True)
    
    
    combined_df = fstpy.metadata_cleanup(combined_df)
    combined_df = combined_df.drop(['path','key','shape'], axis=1)
    cols.remove('path') if 'path' in cols else do_nothing()
    cols.remove('key') if 'key' in cols else do_nothing()
    cols.remove('shape') if 'shape' in cols else do_nothing()
    combined_df[cols]
    
    def load_data(df):
        for i in df.index:
            df.at[i,'d'] = fstpy.to_numpy(df.at[i,'d'])
        return df
    
    already_computed = []
    # write individual variables to separate files
    groups = combined_df.groupby(['grid'])
    for grid, var_df in groups:
        meta_df = combined_df.loc[combined_df.nomvar.isin(['>>','^^', '^>']) & (combined_df.grid == grid)]
        for nomvar in list(var_df.nomvar):
            if (nomvar == '>>') or (nomvar == '^^') or (nomvar == '^>'):
                continue
            
            var_save_path = ''.join([save_path, '/'+ nomvar + '.std'])
            # add positional records for graphical display
            current_var_df = var_df.loc[var_df.nomvar == nomvar]
            to_write_df = pd.concat([meta_df,current_var_df], ignore_index=True)
            
            #load_data function converts dask array to numpy array
            to_write_df = load_data(to_write_df)
            
            #After the dask arrays have been converted to numpy, save them so we don't have to convert again
            already_computed.append(to_write_df)
            fstpy.delete_file(var_save_path)
            fstpy.StandardFileWriter(var_save_path, to_write_df).to_fst()
        
        
    #Read UU and VV files after saving to make UV; saves memory this way
    if atm_variables2:
        for i in range(0, len(atm_variables2), 2):
            vector1_name = atm_variables2[i]
            vector2_name = atm_variables2[i+1]
            hypot_save_path = ''.join([save_path, '/'+ vector1_name[0] + vector2_name[0] + '.std'])
            file1 = ''.join([save_path, '/'+ vector1_name + '.std'])
            file2 = ''.join([save_path, '/'+ vector2_name + '.std'])
            v1_df = fstpy.StandardFileReader(file1, decode_metadata=True).to_pandas()
            meta_df = v1_df.loc[v1_df.nomvar.isin(['>>','^^', '^>'])]
            v1_df = v1_df.loc[(v1_df.nomvar==vector1_name) & (v1_df.surface==True)]
            v2_df = fstpy.StandardFileReader(file2, decode_metadata=True).to_pandas()
            v2_df = v2_df.loc[(v2_df.nomvar==vector2_name) & (v2_df.surface==True)]
            v1_data = v1_df.iloc[0].d
            v2_data = v2_df.iloc[0].d
            hypot = pd.DataFrame([v1_df.iloc[0].to_dict()])
            hypot['nomvar'] = atm_variables2[i][0] + atm_variables2[i+1][0]
            hypot_data = np.hypot(v1_data, v2_data)
            hypot.at[0, 'd'] = hypot_data
            write_df = pd.concat([meta_df, hypot], ignore_index = True)
            already_computed.append(write_df)
            fstpy.delete_file(hypot_save_path)
            fstpy.StandardFileWriter(hypot_save_path, write_df).to_fst()
            
    
    
    # write the complete dataframe with all nomvar variables
    all_results_df = pd.concat(already_computed, ignore_index=True)
    combined_save_path = ''.join([save_path, '/combined.std'])
    fstpy.delete_file(combined_save_path)
    fstpy.StandardFileWriter(combined_save_path, all_results_df).to_fst()
    print("\n", "Finished writing rpn files")
    
    #Create graphs using MatPlot
    import fstd2nc
    ds = fstd2nc.Buffer(combined_save_path).to_xarray()
    graphics = os.path.join(graphics_save_path,'file.nc')
    #Save netcdf file 
    print("Saving netcdf file")
    ds.to_netcdf(graphics, 'w')
    print("\n",  "Creating graphs")

    print(list(ds.keys()))
    print(atm_variables)
    
    for var in list(ds.keys()):
        if var not in atm_variables:
            continue
        print("Saving ", var)
        ds[var].plot(size=15, cmap='jet')
        plt.title(f"{var}")
        
        if os.path.isfile('/'.join([graphics_save_path,var + '.jpg'])):
            os.remove('/'.join([graphics_save_path,var + '.jpg']))
        plt.savefig('/'.join([graphics_save_path,var + '.jpg']))
        plt.clf()
    
    print("Finished")
if __name__ == '__main__':
    main()
