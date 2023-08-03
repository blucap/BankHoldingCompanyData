#!/home/yourname/anaconda3/bin/python
#!/usr/bin/env python3

import glob
import os
# import re
import pandas as pd
from termcolor import colored
pd.set_option('display.max_columns', None)
# import base64
# import warnings
# import sys, string, subprocess
#import argparse
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
# from sys import platform as _platform


def check_file_exists(longfname, rw):
    return(os.path.isfile(longfname))


def makelable_dict(df):
    lijst_df_rssd = list(set([x for x in list(df) if 'RSSD' in x]))
    lijst_df_bhck = list(set([x[-4:] for x in list(df) if not 'year' in x and not 'qid' in x if not 'RSSD' in x]))

    dfd = pd.read_csv("../MDRM_CSV.csv", encoding="ISO-8859-1", low_memory=False, parse_dates=False, infer_datetime_format=False, skiprows=[0])
    dfd = dfd.loc[(dfd['Reporting Form'] == "FR Y-9C") | (dfd['Reporting Form'] == "FR Y-9C"), ['Mnemonic', 'Item Code', 'Item Name']]
    dfd = dfd.rename(columns={'Item Code': 'item', 'Item Name': 'label', 'Mnemonic': 'mnem'}).drop_duplicates(subset=['item'])#.set_index('item')
    dfd = dfd.assign(long_mnem = dfd.mnem + dfd.item)

    df_rssd = pd.DataFrame(index=lijst_df_rssd).join(dfd.set_index('long_mnem')['label']).dropna()
    df_FRY9C = pd.DataFrame(index=lijst_df_bhck).join(dfd.set_index('item')['label']).dropna()

    df_lables = pd.DataFrame(list(set([x for x in list(df) if not 'year' in x and not 'qid' in x if not 'RSSD' in x])), columns = ['long_key'])
    df_lables['item'] = df_lables['long_key'].str[-4:]
    df_lables = df_lables.set_index('item').join(df_FRY9C).reset_index().dropna()
    df_lables = df_lables[['long_key', 'label']].set_index('long_key')
    df_lables.index.names = ['item']

    df_lables = pd.concat([df_lables, df_rssd])
    df_lables['label'] = df_lables['label'].str[:50]
    df_lables = df_lables.to_dict()
    return df_lables['label']

# df_lables = makelable_dict(df)


def concat_pieces(pieces, fname, featherfile, statafile, label):
    if len(pieces) > 0:
        df   = pd.concat(pieces, ignore_index=True, sort=False)  # changed  20180716 (sort=False)
        df   = order(df, ['RSSD9001', 'RSSD9999', 'year', 'qid'])
        print('Writing to cvs.gz')
        df.to_csv(fname + '.gz', index=False, sep="^", compression='gzip')
        print('Writing to featherfile')
        df.to_feather(featherfile)
        if statafile != "0":
            print('Writing to Stata')
            df.to_stata(statafile, version=119, variable_labels=makelable_dict(df))
        print('\n%s has a count of %s.' % (label, df["qid"].count()))
    else:
        print(label + "leeg")
    return df


def order(frame, var):
    varlist = [w for w in frame.columns if w not in var]
    frame = frame[var + varlist]
    return frame


def delfile(file_to_delete):
    if file_to_delete != "0":
        try:
            os.remove(file_to_delete)
        except:
            pass


def writefilesorted(list_to_write, pad, file_to_write):
    with open(pad + file_to_write, mode='wt') as myfile:
        myfile.write('\n'.join(list_to_write))


def qtr(x):
    return {'0331': 1, '0630': 2, '0930': 3, '1231': 4}[x]


def read_vars(pad, fname, up):
    columns = pd.read_csv(pad + fname, header=None).apply(lambda x: x.str.strip())
    columns = [x for x in columns[0].to_list()]
    if up == "U":
        columns = [x.upper() for x in columns]
    return columns


def skip_bad_file(fname):
    with open(fname) as f:
        lines = list(f)
    g = open(fname, "w")
    for line in lines:
        data = line.split('^')
        if data[0] != "1115406":
            g.write(line)
        else:
            print(colored("\nSkipped the bad line in 2003 Q 1.\n", 'red'))
    g.close()


#   main(pad, path, bank_out_file, var_out_file, panelfile,              vars_in_file, filesfile, statafile, add2db, user, password, host):
def main(pad, path, bank_out_file, var_out_file, panelfile, featherfile, vars_in_file, filesfile, statafile):  # , add2db, user, password, host):
    banks = []
    variables = []
    all_pieces = []
    filecount = 0
    tot_filecount = 0

    tgt_list = read_vars(pad, vars_in_file, "U")

    if check_file_exists(pad + filesfile, "r"):
        lyst = read_vars(pad, filesfile, "L")
        add_pad = 1
    else:
        lyst = sorted(glob.glob(path))
        add_pad = 0

    tot_filecount = len(lyst)
    print(('\nTotal number of files submitted: %s.\n') % tot_filecount)
    for naam in lyst:
        if add_pad == 1:
            fname = pad + naam
        else:
            fname = naam
        print("File: " + fname, end='')
        if check_file_exists(fname, "r"):
            if "bhcf0303.txt" in fname:
                print(colored("-", 'red'))
                skip_bad_file(fname)
            else:
                print("+")
            f = open(fname, 'r')
            filecount += 1
            with f:
                word = f.readline().upper().split('^')
                if word[0] == "RSSD9001":
                    idx = 0
                if word[1] == "RSSD9001":
                    idx = 1
                #some files have dashes on line 2, but not all files
                rowcount = 0
                for line in f:
                    data = line.split('^')
                    if data[0] != "--------":
                        if data[idx] not in banks:
                            banks.append(data[idx])
                    else:
                        rowcount += 1
            f.close()
            # Read date in frame, use the idx to make sure the right rows are imported
            # For presice I set parse_dates=False, infer_datetime_format=False
            if rowcount != 0:
                dfr = pd.read_csv(fname, sep='^', encoding="ISO-8859-1", low_memory=False, parse_dates=False, infer_datetime_format=False, skiprows=[rowcount], engine="c")
            else:
                dfr = pd.read_csv(fname, sep='^', encoding="ISO-8859-1", low_memory=False, parse_dates=False, infer_datetime_format=False, engine="c")
            #Set vars to upper case
            for item in list(dfr):
                if not item == item.upper():
                    dfr = dfr.rename(columns={item: item.upper()})
                if item not in variables:
                    variables.append(item)
            #All variables of the frame are here:
            var_list = list(dfr)
            #Assing the main id variables
            dfr['year'] = int(str(dfr["RSSD9999"].loc[0])[0:4])
            dfr['qid']  = qtr(str(dfr["RSSD9999"].loc[0])[4:9])

            print('Rowcount: %2s, Cell 1: %10s. Cell 2: %10s. Cell 3: %10s.' %  (rowcount, var_list[0], var_list[1], var_list[2]), end='')
            print(' RSSD9999: ', end='')
            print(colored(dfr["RSSD9999"].loc[0], 'red'), end='')
            print(' Year: %5s. Qtr: %2s. Count %4s. Vars: %5s' %  (dfr["year"].loc[0], dfr["qid"].loc[0], dfr["RSSD9001"].count(), len(var_list)), end='')
            print(' Banks: %5s.' %  len(banks), end='')
            print(' Variables: %5s.' %  len(variables), end='')
            print(' Filecount: %4s of %4s.\n' %  (filecount, tot_filecount), end='')

            tmp_list = ['RSSD9001', 'RSSD9999', 'year', 'qid']
            for item in tgt_list:
                if item in var_list:
                    if item not in tmp_list:
                        tmp_list.append(item)
                        print(colored(item + " ", 'green'), end='')
                else:
                    print(colored(item + " ", 'red'), end='')
            # print("\nNote: please update mdrm_scraper.\n")
            dfr = dfr[tmp_list]
            all_pieces.append(dfr)
        else:
            print("\nThis BHC file is missing:"),
            print(colored(fname, 'red'), end='')
            print("I will skip.\n")
            filecount += 1

    df = concat_pieces(all_pieces, panelfile, featherfile, statafile, 'Panel')

    print("Totals")
    print('Banks:    %s.' % len(banks))
    print('Variables: %s.' % len(variables))
    #print("\n")

    #Now write to files
    writefilesorted(banks,     pad, bank_out_file)
    writefilesorted(variables, pad, var_out_file)
    print("Done!!!!\n")
    return df


if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-pad", "--pad",                        default=os.getcwd(), help="Set path of your files")
    parser.add_argument("-bank_out_file", "--bank_out_file",    default='banks.csv', help='Set file name for banks output file. Enter 0 if not required. Default is "banks.csv".')
    parser.add_argument("-var_out_file",  "--var_out_file",     default='variables.csv', help='Set file name for variables output file. Enter 0 if not required. Default is "variables.csv".')
    parser.add_argument("-panelfile",  "--panelfile",           default='panelfile.csv', help='Set file name for panel file. "panelfile.csv".')
    parser.add_argument("-featherfile",  "--featherfile",       default='panelfile.feather', help='Set file name for feather file. "panelfile.feather".')
    parser.add_argument("-vars_in_file",  "--vars_in_file",     default='bhc_vars.csv', help='Set file name for variables input file. Default is "bhc_vars.csv".')
    parser.add_argument("-filesfile",  "--filesfile",           default='lyst3.csv', help='Set file name for files input file. Default is "lyst3.csv".')
    parser.add_argument("-statafile",  "--statafile",           default='panelfile.dta', help='Set file name for stata output. Enter 0 if not required. Default is "panelfile.dta".')
    args = vars(parser.parse_args())

    pad = args["pad"] + "/"
    path = pad + "bhcf*.txt"

    print("\nInputs:")
    for key, value in args.items():
        print(key, value)
    print("\n")

    df = main(pad, path, args["bank_out_file"], args["var_out_file"], args["panelfile"], args["featherfile"], args["vars_in_file"], args["filesfile"], args["statafile"])  # , args["add2db"], args["user"], args["password"], args["host"])
