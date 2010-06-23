#!/usr/bin/perl
# $Id$
#
# Copyright (c) 2007 Dennis S.Davidoff <davydov@nexo.ru>,
# Moscow, Russia.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

use Socket;
use DBI;
use POSIX 'strftime';
use Getopt::Std;
use Sys::Syslog qw(:DEFAULT setlogsock);
use Net::Netmask;
use strict;
use vars qw( $dbh );

++$|;
$0 =~ s{.*/}{};
my $DEBUG = 0;
my $PIDFILE = '/var/run/cnupmsave.pid';
my $SYSLOG_FACILITY = 'user';

my $CNUPM_HDR_FMT 	= 'N4';
my $CNUPM_REC_FMT 	= 'C2n2x2Nx12Nx12NN';
my $CNUPM_VERSION 	= 3;
my $CNUPM_HDR_LEN 	= 16;
my $CNUPM_REC_LEN 	= 48;

my $DBNAME 		= 'mydatabase';
my $DBHOST 		= '192.168.123.149';
my $DBUSER 		= 'mylogin';
my $DBPASS 		= 'mypasswd';
my $DBSCHEME 		= 'public';
my $TABLE 		= "$DBSCHEME.user_traf";
my $TABLE_DETAIL	= "$DBSCHEME.user_traf_dtl";

sub usage;
sub table_exists($);
sub rate($$);
sub human($);
sub readpid($);
sub writepid($);
sub is_running($);

setlogsock('unix');
openlog($0, 'cons,pid', $SYSLOG_FACILITY);

$SIG{INT} = $SIG{ABRT} = $SIG{TERM} = sub {
	$dbh->disconnect if $dbh;
	syslog('warning', 'Aborted');
	exit 1;
};

sub usage {
	die "Usage: $0 [-ev] cnupm-ifX.dump cidr\n";
}

sub table_exists($) {
	my ($table) = ($_[0] =~ /([^.]+)$/);
	return defined(($dbh->func($table, 'table_attributes'))->[0]);
}

sub rate($$) {
	print STDERR sprintf("\b"x4 . "%3d%%", (100 * $_[0])/$_[1]);
} 

sub human($)
{
	my $size = shift;
	my $suffix = '';

	return '0B' if ($size == 0);
	if ($size >= 1024 && $size < 1048576) {
		$suffix = 'K'; $size /= 1024;
	} elsif ($size < 1073741824) {
		$suffix = 'M'; $size /= 1048576;
	} elsif ($size < 1099511627776) {
		$suffix = 'G'; $size /= 1073741824;
	} elsif ($size < 1125899906842624) {
		$suffix = 'T'; $size /= 1099511627776;
	} else {
		$suffix = 'P'; $size /= 1125899906842624;
	}

	return sprintf((($size >= 10) ? "%.0f%s" : "%.1f%s"), $size, $suffix);
}

sub writepid($) {
	my $f = shift;
	local *PID;
	unless (open PID, '>'.$f) {
		syslog('notice', "Can't write pid to $f: $!");
		die "$0: Can't write pid to $f: $!\n";
	}
	print PID $$;
	close PID;
}

sub readpid($) {
	my $f = shift;
	local *PID;
	die "$0: $f is not defined!\n" unless defined $f;
	open PID, '<'.$f or die "Can't read pid from $f: $!\n";
	my $pid = <PID>; ($pid) =~ /^(\d+)/;
	close PID;
	return $pid;
}

sub is_running($) {
	my $pid_file = shift;
	my $pid = readpid($pid_file);
	return kill 0, $pid;
}

-e $PIDFILE && is_running($PIDFILE) ? die "$0: is already running!\n" : unlink $PIDFILE;
writepid($PIDFILE);

my %opts=();
getopts('ev', \%opts);
++$DEBUG if $opts{'v'};

my $cnupmfile = shift || usage;
my $cidr = shift || usage;
usage unless -f $cnupmfile;

my $cnupmfile_err = $cnupmfile . '.err';
open ERRREP, '>>' . $cnupmfile_err if $opts{'e'};

my $block = new2 Net::Netmask($cidr) || die "$0: Invalid cidr: $cidr\n";
my $fsize = human(-s $cnupmfile);

syslog('info', "$cnupmfile ($fsize), $cidr");
warn "Setting filter for network $cidr\n" if $DEBUG;
warn "Connecting to database $DBHOST (database $DBNAME) and preparing statements\n"
	if $DEBUG;

$dbh = DBI->connect(
	"DBI:Pg:dbname=$DBNAME;host=$DBHOST", $DBUSER, $DBPASS,
	{ RaiseError => 1, AutoCommit => 0 }); 
undef($DBUSER);
undef($DBPASS);

if (!table_exists($TABLE)) {
	die "$0: Database table $TABLE doesn't exists!\n";
}

if (!table_exists($TABLE_DETAIL)) {
	die "$0: Database table $TABLE_DETAIL doesn't exists!\n";
}

my $upd = $dbh->prepare(qq{
	UPDATE $TABLE
	SET updated = NOW(), inbound=inbound+?, outbound=outbound+?
	WHERE ipaddr = ? AND dt = ?
});

my $ins = $dbh->prepare(qq{
	INSERT INTO $TABLE
	(inbound,outbound,ipaddr,dt,updated) VALUES (?,?,?,?, NOW())
});

my $upd_d = $dbh->prepare(qq{
	UPDATE $TABLE_DETAIL SET bytes = bytes + ?, updated=NOW()
	WHERE src = ? AND dst = ? AND dt = ?
});

my $ins_d = $dbh->prepare(qq{
	INSERT INTO $TABLE_DETAIL (dt,src,dst,bytes,updated)
	VALUES (?,?,?,?,NOW())
});

warn "Processing $cnupmfile ($fsize)\n" if $DEBUG;
open CNUPM, "< $cnupmfile" or die "Can't open $cnupmfile: $!\n";
binmode CNUPM;

my %traffic = ();
my %traffic_dtl = ();
my ($hdr, $rec, $k);
my $total_incoming = 0;
my $total_outgoing = 0;
my $nonmatched = 0;
  
while(($k = read(CNUPM, $hdr, $CNUPM_HDR_LEN)) == $CNUPM_HDR_LEN)
{
	my ($flags, $start, $stop, $count) = unpack($CNUPM_HDR_FMT, $hdr);
	my $ver = sprintf("%u.%u", $flags & 0xff, ($flags >> 8) & 0xff);

	die "$0: $cnupmfile: Incompatible version of dump file: ".
	    ($flags & 0xff) ."\n" if ($flags & 0xff) > $CNUPM_VERSION;

	my $stop_ = strftime("%Y-%m-%d", localtime($stop));
	my $inf = strftime("%Y/%m/%d %H:%M:%S", localtime($start)) . '-'
	    . strftime("%Y/%m/%d %H:%M:%S", localtime($stop))
	    . ", v$ver, $count records";
	print STDERR "$inf     " if $DEBUG;
	print ERRREP "$inf\n" if $opts{'e'};

	$k = $CNUPM_REC_LEN;
	foreach (1 .. $count)
	{
		&rate($_, $count) if $DEBUG;
		last if ($k = read(CNUPM, $rec, $CNUPM_REC_LEN)) != $CNUPM_REC_LEN;
		# XXX: meanwhile not all variables used :)
		my ($family, $proto, $sport, $dport, $src, $dst,
		    $bytes_high, $bytes_low) = unpack $CNUPM_REC_FMT, $rec;
		my $bytes = $bytes_high * 0x100000000 + $bytes_low;
		$src = inet_ntoa(pack("N", $src));
		$dst = inet_ntoa(pack("N", $dst));
		$proto = (getprotobynumber($proto))[0];
		my $direction;
		my $ipaddr;

		#if ($block->match($dst) && !$block->match($src)) {
		if ($block->match($dst)) {
			$direction = 'incoming';
			$ipaddr = $dst;
			$total_incoming += $bytes;
		#} elsif ($block->match($src) && !$block->match($dst)) {
		} elsif ($block->match($src)) {
			$direction = 'outgoing';
			$ipaddr = $src;
			$total_outgoing += $bytes;
		} else {
			print ERRREP "$family $proto $src:$sport $dst:$dport $bytes\n" if $opts{'e'};
			++$nonmatched; 
			next; 
		}

		$traffic{ $stop_ }->{$ipaddr}{$direction} += $bytes;
		$traffic_dtl{ $stop_ }->{"$src;$dst"}{'bytes'} += $bytes;
	}
	last if $k != $CNUPM_REC_LEN;
	print STDERR "\n" if $DEBUG;
}
close CNUPM;

warn "Qty non-matched records for the $cidr: $nonmatched\n"
	if $DEBUG && $nonmatched != 0;
die "\n$0: $cnupmfile: File corrupt\n" if $k > 0;
die "\n$0: $cnupmfile: $!" if $k < 0;

print STDERR "Processing finished for $cnupmfile (incoming ", 
    human($total_incoming), ", outgoing ". human($total_outgoing) .")\n" if $DEBUG;

print STDERR "Uploading traffic into database ... " if $DEBUG;    

my $i_dtl = 0;
foreach my $tm (keys %traffic_dtl)
{
	foreach (keys %{ $traffic_dtl{$tm} }) {
		my ($src, $dst) = split /;/;
		$upd_d->execute($traffic_dtl{$tm}->{$_}{'bytes'}, $src, $dst, $tm);
		$ins_d->execute($tm, $src, $dst, $traffic_dtl{$tm}->{$_}{'bytes'}) if !$upd_d->rows;
		++$i_dtl;
	}
}
undef %traffic_dtl;

my $i = 0;
foreach my $tm (keys %traffic)
{
	my $total_in = 0;
	my $total_out = 0;
	foreach my $ip (keys %{ $traffic{$tm} }) {
		my $in = $traffic{$tm}->{$ip}{'incoming'} || 0;
		my $out = $traffic{$tm}->{$ip}{'outgoing'} || 0;
		$total_in += $in;
		$total_out += $out;
		$upd->execute($in, $out, $ip, $tm);
		$ins->execute($in, $out, $ip, $tm) if !$upd->rows;
		++$i;
	}
	syslog('notice',
	    "$cidr, $tm, incoming = ". human($total_in) . ", outgoing = ". human($total_out));
}

$dbh->commit;
print STDERR "done\n" if $DEBUG;
printf STDERR "%d records processed in table $TABLE\n", $i if $DEBUG;
printf STDERR "%d records processed in table $TABLE_DETAIL\n", $i_dtl if $DEBUG;

print STDERR "VACUUM tables $TABLE and $TABLE_DETAIL ... " if $DEBUG;
$dbh->{AutoCommit} = 1;
$dbh->do(qq{ 
	VACUUM ANALYZE $TABLE;
	VACUUM ANALYZE $TABLE_DETAIL;
});
print STDERR "done\n" if $DEBUG;

$dbh->disconnect;
unlink($cnupmfile) || die "$0: Cannot delete $cnupmfile: $!\n";
close(ERRREP) if $opts{'e'};
unlink($cnupmfile_err) if -z $cnupmfile_err;
syslog('info', 'Finished');
