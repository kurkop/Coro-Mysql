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
	$self->{TABLES} = ();
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

sub select {
	my ($self,$socket, $query) = @_;
	my $jsons;
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
		my $query_table = $query =~ m/(from\s|FROM\s)(\w+)/;
		$self->{TABLES}->{$2} = () unless $self->{TABLES}->{$2};
		$self->{TABLES}->{$2}->{$query} = $query;
		push(@{$self->{pool}}, $dbhc);
		$socket->send($jsons);
		$self->{sem}->up;
	}
}

sub insert {
	my ($self,$socket, $query) = @_;
	$self->exec($socket,$query);
	my $table = (split(" ",$query))[2];
	#warn "KEY $_ and VALUE ".$self->{TABLES}->{$table}->{$_} for (keys(%{$self->{TABLES}->{$table}}));
	#warn "Before clean cache";
	#warn $_ for (keys(%{$self->{CACHE}}));
	#Clean cache
	delete $self->{CACHE}->{$_} for (keys(%{$self->{TABLES}->{$table}}));
	#warn "After clean cache";
	#warn $_ for (keys(%{$self->{CACHE}}));
	#warn "KEY $_ and VALUE ".$self->{TABLES}->{$table}->{$_} for (keys(%{$self->{TABLES}->{$table}}));
	#Clean tables
	delete $self->{TABLES}->{$table};
	warn $self->{TABLES}->{$table};
}

sub update {
	$_[0]->exec($_[1],$_[2]);	
}

sub exec {
	my ($self,$socket, $query) = @_;
	$self->{sem}->down while (!(scalar(@{$self->{pool}})));
	my $dbhc = shift @{$self->{pool}};
	my $sth = $dbhc->prepare($query);
	$sth->execute;
	push(@{$self->{pool}}, $dbhc);
	$self->{sem}->up;
	$socket->send("\n");
}

sub query {
	my ($self,$socket, $args) = @_;
	my $query = $args->[1];
	$query = substr $query, 1, -1;
	my $command = (split(" ",$query))[0];
	#Execute query
	($self->select($socket, $query) & return) if ($command eq "select");
	($self->insert($socket, $query) & return) if ($command eq "insert");
	($self->update($socket, $query) & return) if ($command eq "update");
	$self->select($socket, $query);
}

sub dispatcher {
	my ($self,$socket, $cmd) = @_;
	my @args = $cmd =~ /"[^"]*"|\S+/g;
	my $dispatch = $args[0];
	#Execute self method
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

Run server:

	use Coro::Mysql::cache_server;

	$server = Coro::Mysql::cache_server->new();
	$server->run("database","0.0.0.0",5000, 100);
	EV::loop;

Run client:

	use Coro::Mysql::cache_client;

	$client = Coro::Mysql::cache_client->new();
	$client->connect("localhost", 5000);
	my $command = $client->query("select * from database order by id desc limit 10");
	print $client->ping();
	$client->set("Key1","Key2");
	print $client->get("Key1");
	$client->del("Key1");
	$client->disconnect();

=head1 DESCRIPTION

This module is a cache key/value implementation to Mysql using
L<Coro::Mysql> and L<AnyEvent::Socket>.

This module implements Coro per connection, using Anyevent for create
each connection. In each event use Socket Handle as a local variable
to recieve message from client (see Coro::Mysql::cache_client) to
execute commands and return responses.

