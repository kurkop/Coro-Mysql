#!/usr/bin/perl -w
package Coro::Mysql::cache_server;
use strict;

use Coro;
use AnyEvent::Socket;
use Coro::Handle;
use EV;
use Coro::Mysql;
use DBI;
use Coro::Debug;
use Time::Format qw/%time/;
use Time::HiRes qw/gettimeofday usleep tv_interval/;

sub new {
	my $class = ref($_[0]) || $_[0];
	
	my $self = {};
	$self->{pool} = [];
	$self->{CACHE} = ();
	$self->{sem} = new Coro::Semaphore 0;
	$self->{t0} = [gettimeofday];

	bless $self, $class;
	return $self;
}

sub ping {
	my ($self,$socket) = @_;
	$socket->send("pong\n");
}

sub set {
	my ($self,$socket, $args) = @_;
	$self->{CACHE}->{$args->[1]}= $args->[2];
	#for my $key (keys(%{$self->{CACHE}})){
	#	warn "Key is $key and value is ".$self->{CACHE}->{$key};
	#}
}

sub get {
	my ($self,$socket, $args) = @_;
	$socket->send($self->{CACHE}->{$args->[1]}."\n");
}

sub del {
	my ($self,$socket, $args) = @_;
	delete $self->{CACHE}->{$args->[1]};
}

sub quit {
	my ($self,$socket, $args) = @_;
	close $socket;
	my $elapsed = tv_interval ( $self->{t0}, [gettimeofday]);
	warn "Quit $elapsed";
}

sub fetchrow_array_json {
	my ($self, $sth) = @_;

	my $names = $sth->{'NAME'};
	my $numFields = $sth->{'NUM_OF_FIELDS'} - 1;
	my $jsons = "";

	while (my $row = $sth->fetchrow_arrayref) {
		$jsons .= "{";
		for ( 0..$numFields ) {
			$jsons .= " ".$$names[$_].": '".$$row[$_]."'";
			$jsons .= "," if ($_<$numFields);
      	}
      	$jsons .= "}";
	}
	return $jsons."\n";
}

sub query {
	my ($self,$socket, $args) = @_;
	my $query = $args->[1];
	my $jsons;
	$query = substr $query, 1, -1;
	
	if ( $self->{CACHE}->{$query} ){
		$jsons = $self->{CACHE}->{$query};
		$socket->send($jsons);
	}
	else {
		$self->{sem}->down while (!(scalar(@{$self->{pool}})));
		my $dbhc = shift @{$self->{pool}};
		my $sth = $dbhc->prepare($query);
		$sth->execute;
		$jsons = $self->fetchrow_array_json($sth);
		$self->{CACHE}->{$query} = $jsons;
		push(@{$self->{pool}}, $dbhc);
		$socket->send($jsons);
		$self->{sem}->up;
	}	
}

sub dispatcher {
	my ($self,$socket, $cmd) = @_;
	my @args = $cmd =~ /"[^"]*"|\S+/g;
	my $dispatch = $args[0];
	$self->$dispatch($socket,\@args);
}

#Datatbase name, Host, Port, Socket to database
sub run {
	my ($self, $database, $host, $service, $socketsdb) = @_;

	for(1..$socketsdb){
		push (@{$self->{pool}}, DBI->connect ("DBI:mysql:$database", "root", "")->Coro::Mysql::unblock);
	}

	tcp_server $host, $service || 10000, sub {
		my ($fh, $host, $port) = @_;
		async{
			my $client = unblock $fh;
			while(<$client>){
				$self->dispatcher($client,$_);	 
				last if (/quit/i);
			}
		};
		cede;
	};
}

1;

=head1 NAME

Coro::Mysql::cache_server

=head1 SYNOPSIS

use Coro::Mysql::cache_server

#Create cache_server
$server = Coro::Mysql::cache_server->new();

#Run server (Datatbase name, Host, Port, Number connection to database)
$server->run("app46","0.0.0.0",5000, 100);

#Run Loop
EV::loop;

=head1 DESCRIPTION

This module is a cache key/value implementation to Mysql using
L<Coro::Mysql> and L<AnyEvent::Socket>.

This module implements Coro per connection in a event created by Anyevent.

=head1 TESTING

Connect with Telnet to server:

