import glob
import os
import re
import pandas as pd
from termcolor import colored
pd.set_option('display.max_columns', None)
import base64
import MySQLdb
import warnings 
import sys
import argparse
from sys import platform as _platform
# suppress annoying mysql warnings
warnings.filterwarnings(action='ignore', category=MySQLdb.Warning) 

def ops():
	if _platform == "linux" or _platform == "linux2":
		opersys = "linux"
	elif _platform == "darwin":
		opersys = "darwin"
	elif _platform == "win32":
		opersys = "win"
	return opersys

def check_file_exists(longfname,rw):
	success=False
	if rw in ["r","w"]:
		try:
			f = open(longfname, rw)
			f.close()
			success=True
		except:
			print('\nFilename is not valid: %s') % longfname
			success=False
	return success

def concat_pieces(pieces,fname,statafile,label,dblabel,add2db,user, password, host):
	#add_frame_to_db(all_float_pieces,'FLOAT_VARS')
	if len(pieces)>0:
		df   = pd.concat(pieces, ignore_index=True)
		df   = order(df,['RSSD9001', 'RSSD9999', 'year', 'qid'])
		df.to_csv(fname, index=False, sep="^")
		if statafile!="0": 
			if check_file_exists(statafile,"w"):
				df.to_stata(statafile)
		print '\nLabel %s has a count of %s.' % (label, df["qid"].count())
		if add2db==1:
			add_frame_to_db(df,  dblabel.upper(), user, password, host)
	else:
		print label+"leeg"

def order(frame,var):
	varlist =[w for w in frame.columns if w not in var]
	frame = frame[var+varlist]
	return frame 

def add_frame_to_db(frame, table, user, password, host):
	add_to_db(frame,table,user, password, host)
	lijstje=[]
	for litem in list(frame):
		lijstje.append(litem)
	modify_col_type(lijstje, table, frame,user, password, host) 

def add_to_db(frame,table,user, password, host):
	db = MySQLdb.connect(user=user, passwd=password, db="BHC")
	frame.to_sql(con=db, name=table, if_exists='replace', flavor='mysql',index=False)
	#fram2 = frame.astype(object).where(pd.notnull(frame), None)
	#frame[:1].to_sql(con=db, name=table, if_exists='replace', flavor='mysql',index=False)
	#fram2[1:].to_sql(con=db, name=table, if_exists='append', flavor='mysql',index=False)
	
	
def modify_col_type(head_list, table_name, frame,user, password, host):
	db = MySQLdb.connect(user=user, passwd=password, db="BHC")
	for hitem in head_list:
		if int(str(frame[hitem].dtype).find("int"))>-1:
			top = frame[hitem].max()
			bot = frame[hitem].min()
				#print "Top %12s, Bot %12s" % (top, bot)
			if top-bot>top:
				top = top-bot
			#print "New top %12s" % top
			if top>2147483647:
				int_big=" bigint("
			else:
				int_big=" int("	 
			sqlstring= "ALTER TABLE "+table_name+" MODIFY "+hitem.lower()+int_big+str(len(str(top)))+")"
		if int(str(frame[hitem].dtype).find("float"))>-1:
			sqlstring= "ALTER TABLE "+table_name+" MODIFY "+hitem.lower()+" double"
		if "sqlstring" in locals():
			print sqlstring
			try:
				db.query(sqlstring)
				## db.query("""ALTER TABLE `BHC_BHCK2170` MODIFY year int(5)""")
			except:
				pass

def delfile(file_to_delete):
	if file_to_delete!="0":
		try:
			os.remove(file_to_delete)
		except:
			pass
	  
def writefilesorted(list_to_write, pad, file_to_write):
	if file_to_write!="0":
		if check_file_exists(pad+file_to_write,"w"):
			list_to_write.sort()
			g = open(file_to_write,"w")
			for item in list_to_write:
				g.write("%s\n" % item)
			g.close()
	
def qtr(x):
	return {
		'0331': 1,
		'0630': 2,
		'0930': 3,
		'1231': 4,
	}[x]

def read_vars(pad,fname,up):
	columns=[]
	if check_file_exists(pad+fname,"r"):
		f = open(pad+fname, 'r')
		for line in f:
			splitline=line.split(',')
			for item in splitline:
				a = item.strip()
				if up=="U":
					a=a.upper()
				if a not in columns and len(a)>0:
					columns.append(a)
	return columns

def main(pad, path, bank_out_file, var_out_file, panelfile, vars_in_file, filesfile, statafile, add2db, user, password, host):
	banks=[]
	variables=[]
	all_pieces=[]

	filecount=0
	tot_filecount=0

	#Clean files
	delfile(pad+bank_out_file)
	delfile(pad+var_out_file)

	tgt_list=read_vars(pad, vars_in_file,"U")
	
	if check_file_exists(pad+filesfile,"r"):
		lyst = read_vars(pad, filesfile,"L")
		add_pad = 1
	else:
		lyst=sorted(glob.glob(path))
		add_pad = 0

	for naam in lyst:
		tot_filecount+=1
	print ('\nTotal number of files submitted: %s.\n') % tot_filecount
	for naam in lyst:
		if add_pad==1:
			fname=pad+naam
		else:
			fname=naam
		print(fname)
		if check_file_exists(fname,"r"):
			f = open(fname,'r')
			filecount+=1
			with f:
				word= f.readline().upper().split('^')
				if word[0]=="RSSD9001":
					idx=0
				if word[1]=="RSSD9001":
					idx=1
				#some files have dashes on line 2, but not all files
				rowcount=0
				for line in f:
					data = line.split('^')
					if data[0]!="--------":
						if data[idx] not in banks:
							banks.append(data[idx])
					else:
						rowcount += 1
			f.close()
			#Read date in frame, use the idx to make sure the right rows are imported
			if rowcount !=0:
				dfr = pd.read_csv(fname, sep='^', low_memory=False, parse_dates=True, infer_datetime_format=True, skiprows=[rowcount])
			else:
				dfr = pd.read_csv(fname, sep='^', low_memory=False, parse_dates=True, infer_datetime_format=True)
			#Set vars to upper case
			for item in list(dfr):
				if not item==item.upper():
					dfr = dfr.rename(columns={item: item.upper()})
				if item not in variables:
					variables.append(item)
			#All variables of the frame are here:
			var_list=list(dfr)
			#Assing the main id variables
			dfr['year'] = int(str(dfr["RSSD9999"].loc[0])[0:4])
			dfr['qid']  = qtr(str(dfr["RSSD9999"].loc[0])[4:9])

			print 'Rowcount:  %2s, Cell 1: %10s. Cell 2: %10s. Cell 3: %10s.' %  (rowcount, var_list[0], var_list[1], var_list[2]),
			print 'RSSD9999:', 
			print colored(dfr["RSSD9999"].loc[0], 'red'),
			print 'Year: %5s. Qtr: %2s. Count %4s. Vars: %5s' %  (dfr["year"].loc[0], dfr["qid"].loc[0], dfr["RSSD9001"].count(), len(var_list)),
			print 'Banks: %5s.' %  len(banks),
			print 'Variables: %5s.' %  len(variables),
			print 'Filecount: %4s of %4s.\n' %  (filecount, tot_filecount)
			
			tmp_list=['RSSD9001', 'RSSD9999', 'year', 'qid']
			for item in tgt_list:
				if item in var_list:
					if item not in tmp_list:
						tmp_list.append(item)
						print colored(item, 'green'),
				else:
					print colored(item, 'red'),
			print "\n"
			dfr=dfr[tmp_list]
			all_pieces.append(dfr)
		else:
			print("\nThis BHC file is missing:"),
			print colored(fname, 'red'),
			print "I will skip.\n" 
			filecount+=1
			
	concat_pieces(all_pieces,panelfile,statafile,'Panel','PANEL_VARS',add2db, user, password, host)

	print "Totals"
	print 'Banks:	 %s.' % len(banks)
	print 'Variables: %s.' % len(variables)
	print "\n"

	#Now write to files
	writefilesorted(banks,	 pad, bank_out_file)
	writefilesorted(variables, pad, var_out_file)
	banks_dfr=pd.DataFrame(banks, columns=['RSSD9001'], dtype="int64")
	if add2db==1:
		add_frame_to_db(banks_dfr,'BANK_IDS', user, password, host)
	print "\nDone!"

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Convert BHC files to verious formats.')
	parser.add_argument('--pad', 			dest='pad', 										help='Set path of your files.')
	parser.add_argument('--bank_out_file', 	dest='bank_out_file', 	default='banks.csv',		help='Set file name for banks output file. Enter 0 if not required. Default is "banks.csv".')
	parser.add_argument('--var_out_file', 	dest='var_out_file', 	default='variables.csv',	help='Set file name for variables output file. Enter 0 if not required. Default is "variables.csv".')
	parser.add_argument('--panelfile', 		dest='panelfile', 		default='panelfile.csv',	help='Set file name for panel file."panelfile.csv".')
	parser.add_argument('--vars_in_file', 	dest='vars_in_file', 	default='bhc_vars.csv',		help='Set file name for variables input file. Default is "bhc_vars.csv".')
	parser.add_argument('--filesfile', 		dest='filesfile', 		default='3banks.csv',		help='Set file name for files input file. Default is "3banks.csv".')
	parser.add_argument('--statafile', 		dest='statafile', 		default='panelfile.dta',	help='Set file name for stata output. Enter 0 if not required. Default is "panelfile.dta".')
	parser.add_argument('--add2db', 		dest='add2db', 			default= 0,					help='Tell me if to output to mysql (1=yes, 2=no). Default  is "0".')
	parser.add_argument('--user', 			dest='user', 			default='root', 			help='The MySQL login username. Default is "root".')
	parser.add_argument('--password', 		dest='password', 		default='',	help='The MySQL hashed login password, see http://stackoverflow.com/questions/157938/hiding-a-password-in-a-python-script')
	parser.add_argument('--host', 			dest='host', 			default='localhost', 		help='The MySQL host. Default is "localhost".')
	args = parser.parse_args(sys.argv[1:])
	if not args.pad:
		args.pad = os.getcwd()
	if not args.add2db:
		args.add2db = 0		
	pad=args.pad
	if ops()=="win":
		if args.pad[-1:]!='\\':
			pad=args.pad+"\\"
	else:
		if args.pad[-1:]!='/':
			pad=args.pad+"/"
	path = pad+"bhcf*.txt"
	
	try:
		add2db = int(args.add2db)
	except:
		add2db = 0
	if not add2db in [0,1]:
		add2db = 0
	
	password = base64.b64decode(args.password)
	main(pad, path, args.bank_out_file, args.var_out_file, args.panelfile, args.vars_in_file, args.filesfile, args.statafile, add2db, args.user, password, args.host)
	