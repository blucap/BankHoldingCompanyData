#!/usr/bin/env perl
#http://search.cpan.org/~msisk/HTML-TableExtract-2.11/lib/HTML/TableExtract.pm
#http://www.federalreserve.gov/reportforms/mdrm/DataDictionary/search_item.cfm?RequestTimeout=900&UserTyped=4769&Series1=All&DataType=1&Selstats=1&DatePeriod=1&StartDateUser=19560101&EndDateUser=99991231&DisplayType=1&DisplayStub=no&DisplayConf=no
#http://www.federalreserve.gov/apps/mdrm/data-dictionary
use strict;
use warnings;
use HTML::TableExtract;

#my $header = q{Some} . chr(0x0A0) . q{ID};
#my $te = HTML::TableExtract->new( headers => [$header] );

my $te = HTML::TableExtract->new( headers => ['Item', 'Description \& Series', 'Confidential'] );

#my $te = HTML::TableExtract->new( depth => 1, count => 0, br_translate => 0, keep_html=>1 );
$te->parse_file('Micro Data Reference Manual.html');
my $row;
my $outfile="MDRM4.csv";
#open(OUT, ">$outfile")|| die "File not found";

 # Examine all matching tables
 foreach my $ts ($te->tables) {
   print "Table (", join(',', $ts->coords), "):\n";
   foreach $row ($ts->rows) {
    for my $cell (@$row) {
        $cell =~ s/^\s*//;
        $cell =~ s/\s*$//;
        $cell =~ s/\R//g;
        $cell =~ s/,//g;
        $cell =~ s/[^\x00-\x7F^\xA1-\xFF]//g;
	$cell =~ s/Short Caption:/,,/g; #replace short cap by two comma's
	#$cell =~ s/\t/ /g;
	$cell =~ s/[ \t]{2,}/ /g; # replace conseq spaces and tab by one
    }
    print join(',', @$row), "\n";
#    print OUT join(',', @$row), "\n";
   }
 }
#close(OUT);