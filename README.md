# BankHoldingCompanyData
BankHoldingCompanyData

I wrote this python script to help download data from the Chicago Federal reserve, specifically from Reporting Form FR Y-9C.

The script downloads the data for all years (if you want to), and then merges the files. It creates STATA output, a CSV file, and it can send the data to MYSQL.

See this page for the source of the data:

https://www.chicagofed.org/banking/financial-institution-reports/bhc-data

PYTHON
The script relies on pandas for python. Please update pandas to the latest version.

INPUTS:
Please run the curl script and unzip the files in the folder where the python script resides is.

The python script uses two input files:

1) One csv file with the variable names that you want, see the example csv file (bhc_vars.csv).

2) One csv file with the bhcf files that you want to process (3banks.csv). 

OUTPUTS:
The python script produces various outputs:

panelfile. csv

banks.csv*

variables.csv*

panelfile.dta (for STATA)*

MYSQL data*

*These you can elect to switch off, see below.

Particulars:

For MySQL you need to rehash your MySQL password, and feed the script the hashed password, see:
http://stackoverflow.com/questions/157938/hiding-a-password-in-a-python-script

See for variable names of the FR Y-9C forms:
http://www.federalreserve.gov/apps/mdrm/data-dictionary

HOW TO RUN:

You can run the script without any parameters, it will then rely on the default vales, see below.

Else you can specify your own settings.

NOTE: the default is NOT to send data to MySql.

USAGE: 

Merge_BHC_files.py [-h] [--pad PAD] [--bank_out_file BANK_OUT_FILE] [--var_out_file VAR_OUT_FILE] [--panelfile PANELFILE] [--vars_in_file VARS_IN_FILE] [--filesfile FILESFILE] [--statafile STATAFILE] [--add2db ADD2DB] [--user USER] [--password PASSWORD] [--host HOST]


Optional arguments:
	-h, --help 
	Show this help message and exit

  --pad PAD 
  Set path of your files.

  --bank_out_file BANK_OUT_FILE
  Set file name for banks output file. Enter 0 if not required. Default is "banks.csv".

  --var_out_file VAR_OUT_FILE
  Set file name for variables output file. Enter 0 if not required. Default is "variables.csv".

  --panelfile PANELFILE 
  Set file name for panel file. Default is "panelfile.csv".

  --vars_in_file VARS_IN_FILE 
  Set file name for variables input file. Default is "bhc_vars.csv".

  --filesfile FILESFILE 
  Set file name for files input file. Default is "3banks.csv".

  --statafile STATAFILE 
  Set file name for stata output. Enter 0 if not required. Default is "panelfile.dta".

  --add2db ADD2DB 
  Tell me if to output to mysql (1=yes, 2=no). Default is "0".

  --user USER 
  The MySQL login username. Default is "root".

  --password PASSWORD 
  The MySQL hashed login password, see http://stackoverflow.com/questions/157938/hiding-a-password-in-a-python-script

  --host HOST 
  The MySQL host. Default is "localhost".
