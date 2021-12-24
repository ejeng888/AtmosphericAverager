This script uses the FSTPY library to calculate an Atmospheric Forcing Average for a given model. 

How to run:
    Load these packages for the FSTPY library to function correctly
    
    . ssmuse-sh -d /fs/ssm/eccc/cmd/cmde/surge/surgepy/1.0.8/
    . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/fstpy/2.1.9/
    . ssmuse-sh -p eccc/crd/ccmr/EC-CAS/master/fstd2nc_0.20211013.0
    . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2
    
    then type: python CallMean_Python.py
    
After starting the program, it should say "Starting....". This part will take a while depending on how many files are in the source directory, but you should eventually see the RPN standard output.


Variables such as save path and source directory can be edited in the CallMean_config file.


The program will output RPN standard files, a Graphic to go along with each variable, and a netCDF file. The Graphic will appear the same way it does in xrec, just without the globe lines.

If you experience an error, check the source directory to ensure that your variables exist within them and that the ip1 value you wrote down exists as well.

Note: If you plan on moving the files around, make sure that the config file is always in the same directory as the Python file and has the name "CallMean_config".

<br />
<br />
<br />

<h1>In Case of Memory Issues</h1>
> Important: When running the program for long periods of time (eg one year), you may experience memory issues. In these cases, it is recommended to split the runs (eg 6 months per run), and then average the resulting files. Remember to use a similar number of files in each run (ideally the same number of files)

> If you wish to use the latter approach, follow these steps: 
  > 1. Place the split files into one directory. Ensure that all the files have the same variables, if they don't the program won't work. For example, if I have one TT.std from Jan-July and another TT.std from Aug-Dec, place them in same directory. Alternatively you can use the combined.std file to make the process quicker
  > 2. Rename the files to match the standard file naming procedure **(YYYYMMDD00_xxx)**, remember to remove .std extension, you can use Linux's mv command to do this. What you name it doesn't matter but ensure that the first file in your directory is written in the start_date section of the config file. End_date can be the last file. Also ensure that _xxx is within the range of the start_time and end_time section of the config file and that '00' is in the include runs list.
  > 3. In the config file, change directory to where you placed the files.
  > 4. In the config file, make sure the vars and ip1 values reflect the variables in your file. When working with vectors, remember to put one of the vectors in vars and the other in is_vector to ensure the hypoteneuse is accurate, do not average the split hypoteneuse's.

> In addition, when doing multiple runs, you should move or rename the combined.std file and the file.nc file so that they do not get overwritten on subsequent runs if you decide to save files to same location.
 
> If you intend on splitting runs, below are some steps to ensure that you get an equal (or at least similar) amount of files in each run:
   > 1. cd 'source directory'
   > 2. $ ls | wc -l (Gets number of files in directory)
   > 3. Divide value by # of runs you need.
   > 4. $ ls | head -n, where n is value obtained in step 3(prints first 'n' files)
   > 5. The last file shown in terminal can be end_date for first run and the subsequent file date can be start_date for second run

> Alternatively, if you know the start and end dates in the file directory, you can put them into a date median calculator online such as [**this one**](https://astrologyhour.com/datesplit.asp)


