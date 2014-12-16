#!/usr/bin/perl -w
package Coro::Mysql::cache_client;
use strict;

use Coro;
use Coro::Handle;
use AnyEvent::Socket;

sub new {
	my $class = ref($_[0]) || $_[0];
	
	my $self = {};
	$self->{pool} = [];
	$self->{CACHE} = ();
	$self->{sem} = new Coro::Semaphore 0;
	$self->{fh} = undef;

	bless $self, $class;
	return $self;
}

sub ping {
	my ($self) = @_;
	return $self->send("ping", 1);
}

sub set {
	my ($self,$key, $val) = @_;
	$self->send("set $key $val");
}

sub get {
	my ($self,$key) = @_;
	return $self->send("get $key", 1);
}

sub del {
	my ($self,$key) = @_;
	$self->send("del $key");
}

sub query {
	my ($self,$query) = @_;
	return $self->send("query \"$query\"", 1);
}

sub send {
	my ($self, $cmd, $return) = @_;
	my $fh = $self->{fh};
	print $fh "$cmd\n";
	return <$fh> if ($return);
}

sub disconnect {
	my ($self) = @_;
	my $fh = $self->{fh};
	print $fh "quit\n";
	close $self->{fh};
}

#Host, Port
sub connect {
	my ($self, $host, $service) = @_;
	tcp_connect $host, $service || 10000, Coro::rouse_cb;
   	$self->{fh} = unblock +(Coro::rouse_wait)[0];
}

1;