=head1 NAME

Coro::Mysql - let other threads run while doing mysql requests

=head1 SYNOPSIS

 use Coro::Mysql;

 my $DBH = Coro::Mysql::unblock DBI->connect (...);

=head1 DESCRIPTION

(Note that in this manual, "thread" refers to real threads as implemented
by the Coro module, not to the built-in windows process emulation which
unfortunately is also called "threads")

This module "patches" DBD::mysql database handles so that they do not
block the whole process, but only the thread that they are used in.

This can be used to make parallel sql requests using Coro, or to do other
stuff while mysql is rumbling in the background.

=head2 CAVEAT

Note that this module must be linked against exactly the same
F<libmysqlclient> library as DBD::mysql, otherwise it will not work.

Also, while this module makes database handles non-blocking, you still
cannot run multiple requests in parallel on the same database handle. If
you want to run multiple queries in parallel, you have to create multiple
database connections, one for each thread that runs queries.

If you make sure that you never run two or more requests in parallel, you
cna freely share the database handles between threads, of course.

Also, this module uses a number of "unclean" techniques (patching an
internal libmysql structure for one thing) and was hacked within a few
hours on a long flight to Malaysia.

It does, however, check whether it indeed got the structure layout
correct, so you should expect perl exceptions or early crashes as opposed
to data corruption when something goes wrong.

=head2 SPEED

This module is implemented in XS, and as long as mysqld replies quickly
enough, it adds no overhead to the standard libmysql communication
routines (which are very badly written).

For very fast queries ("select 0"), this module can add noticable overhead
(around 15%) as it tries to switch to other coroutines when mysqld doesn't
deliver the data instantly.

For most types of queries, there will be no overhead, especially on
multicore systems where your perl process can do other things while mysqld
does its stuff.

=over 4

=cut

package Coro::Mysql;

use strict qw(vars subs);
no warnings;

use Scalar::Util ();
use Carp qw(croak);

use Guard;
use Coro::Handle ();

# we need this extra indirection, as Coro doesn't support
# calling SLF-like functions via call_sv.

sub readable { &Coro::Handle::FH::readable }
sub writable { &Coro::Handle::FH::writable }

BEGIN {
   our $VERSION = '0.1';

   require XSLoader;
   XSLoader::load Coro::Mysql::, $VERSION;
}

=item $DBH = Coro::Mysql::unblock $DBH

This function takes a DBI database handles and "patches" it
so it becomes compatible to Coro threads.

After that, it returns the patched handle - you should always use the
newly returned database handle.

=cut

sub unblock {
   my ($DBH) = @_;
   my $sock = $DBH->{sock};

   open my $fh, "+>&" . $DBH->{sockfd}
      or croak "Coro::Mysql unable to clone mysql fd";

   $fh = Coro::Handle::unblock $fh;

   _patch $sock, $DBH->{sockfd}, tied ${$fh};
   $DBH->{private_Coro_Mysql} = guard {
      _unpatch $sock;
      undef $fh;
   };

   $DBH
}

1;

=back

=head1 AUTHOR

 Marc Lehmann <schmorp@schmorp.de>
 http://home.schmorp.de/

=cut

