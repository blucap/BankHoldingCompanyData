# BankHoldingCompanyData

I wrote this python script to help download data from the Chicago Federal reserve, specifically from Reporting Form FR Y-9C.

The script processes the data for all years (if you want to), and then merges the files. It creates STATA output, a CSV file, and a feather data file.

See this page for the [origins of the data](https://www.chicagofed.org/banking/financial-institution-reports/bhc-data).


**PYTHON**

The script relies on pandas for python. Please update pandas and python to the latest version. Make sure you import the relevant modules.

**INPUTS:**

In the past one would run the curl script and unzip the files in the folder where the python script resides is.

Since a couple of years you should get the data from: [NIC National Information Center](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload)

The python script uses three input files:

1) One csv file with the variable names that you want, see the example csv file (bhc_vars.csv).

2) One csv file with the bhcf files that you want to process (lyst3.csv). 

3) The MDMR file with data definitions: [Micro Data Reference Manual](https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm), which you need to unzip.

**OUTPUTS:**

The python script produces various outputs:

panelfile.csv.gz (this is a compressed csv file to save space and can be opened in pandas without unpacking)

banks.csv

variables.csv

panelfile.dta (for STATA), which you can elect to switch off, see below.

**Particulars:**

See for variable names of the FR Y-9C forms:
http://www.federalreserve.gov/apps/mdrm/data-dictionary

The MDRM data can be scarped using mdrm_scraper.py

**HOW TO RUN:**

You can run the script without any parameters, it will then rely on the default vales, see below.

Else you can specify your own settings.

USAGE: 

    python Merge_BHC_files.py [-h] [--pad PAD] [--bank_out_file BANK_OUT_FILE] [--var_out_file VAR_OUT_FILE] [--panelfile PANELFILE] [--featherfile FEATHERFILE] [--vars_in_file VARS_IN_FILE] [--filesfile FILESFILE] [--statafile STATAFILE] 

Example:
                                 
    python Merge_BHC_files.py  --panelfile 'panelfile_2023.csv' --statafile 'panelfile_2023.dta'  --featherfile 'panelfile_2023.feather'

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
  
    --featherfile FEATHERFILE  

Default = "panelfile.feather"

    --vars_in_file VARS_IN_FILE 

Set file name for variables input file. Default is "bhc_vars.csv".

    --filesfile FILESFILE 

Set file name for files input file. Default is "lyst3.csv".

    --statafile STATAFILE 

Set file name for stata output. Enter 0 if not required. Default is "panelfile.dta".

