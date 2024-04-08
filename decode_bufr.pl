#!/usr/bin/env perl
# PERL version of BUFR decoder using GEO::BUFR instead of ECCODES (but with the same ECCODES tables)
#
# USEFUL LINKS:
# 
# CONDA
# https://dev.to/yzhernand/using-cpanm-to-install-perl-modules-in-a-conda-environment-5ae3
#
# BUFR
# https://metacpan.org/dist/Geo-BUFR/view/lib/Geo/BUFR.pm#BUFR-TABLE-FILES
# https://metacpan.org/release/PSANNES/Geo-BUFR-1.39/view/lib/Geo/BUFR.pm#DECODING/ENCODING
# https://metacpan.org/pod/Geo::BUFR
# https://wiki.met.no/bufr.pm/start
# https://wiki.met.no/bufr.pm/bufrread.pl
#
# YAML
# https://perlmaven.com
# https://www.perl.com/article/29/2013/9/17/How-to-Load-YAML-Config-Files/
# https://metacpan.org/pod/YAML::Tiny
#
# MULTIPROCESSING
# https://metacpan.org/pod/MCE
# https://stackoverflow.com/questions/7931455/is-there-a-multiprocessing-module-for-perl
# https://docstore.mik.ua/orelly/perl4/prog/ch17_01.htm
# https://perlmaven.com/fork
#
# SQLITE
# https://www.tutorialspoint.com/sqlite/sqlite_perl.htm

# Geo::BUFR script are here: ${CONDA_PREFIX}/obs/lib/perl5/5.32/site_perl/Geo

use strict;
use warnings;
use feature 'say';

use Getopt::Long;
use Pod::Usage qw(pod2usage);

use YAML; use YAML::Node; use YAML::XS 'LoadFile';
use Data::Dumper;
use DBI;
use Geo::BUFR;

#TODO implement custom YAML tags (see global_functions/read_yaml)
#OR just ignore them and define types in script
my $config = LoadFile('config/obs.yml');
#print Dumper($config);
my $config_db 	= $config->{database};

my $driver   	= "SQLite"; 
my $database 	= $config_db->{db_file};
my $dsn 	= "DBI:$driver:dbname=$database";
my $userid 	= "";
my $password 	= "";
my $dbh 	= DBI->connect($dsn, $userid, $password, { RaiseError => 1 }) 
	or die $DBI::errstr;

print "Opened database successfully\n";


Geo::BUFR->set_tableformat('ECCODES');
Geo::BUFR->set_tablepath('/home/juri/miniconda3/envs/obs/share/eccodes/definitions/bufr/tables');

my $bufr = Geo::BUFR->new();

$bufr->fopen('/home/juri/data/live/cod/bufr/2309111500_synop3.bufr');
#$bufr->load_BDtables($table);
#$bufr->load_Ctable($table,$default_table);

sub callback {
    my $obj = shift;
    return 1 if $obj->get_data_category != 0;
    my $ahl = $obj->get_current_ahl() || '';
    return ($ahl =~ /^IS.... (ENMI|TEST)/);
}

while ( not $bufr->eof() ) {
	#my $table_version = $bufr->get_table_version($table);
	#print $table_version;
	my ($data, $descriptors) = $bufr->next_observation();
	# $bufr->set_filter_cb(\&callback,@args);
	$bufr->is_filtered();
	print $bufr->dumpsections($data, $descriptors) if $data;
}
 
$bufr->fclose();
