use ExtUtils::MakeMaker;

print <<EOF;

**
** This module *MUST* be linked against exactly the same libmysqlclient
** as your DBD::mysql module. Otherwise it will die quickly when used.
**
** To adjust include and library path, edit the Makefile.PL, near the end.
**

EOF

WriteMakefile(
    dist	=> {
	            PREOP	=> 'pod2text Mysql.pm | tee README >$(DISTVNAME)/README; chmod -R u=rwX,go=rX . ;',
	            COMPRESS	=> 'gzip -9v',
	            SUFFIX	=> '.gz',
	           },
    PREREQ_PM    => {
       Scalar::Util     => 0.00,
       Coro             => 5.1,
       Guard            => 1.0,
       DBI              => 0, # not actually a hard dependency
       DBD::mysql       => 0, # not actually a hard dependency
    },
    NAME => "Coro::Mysql",
    VERSION_FROM => "Mysql.pm",

    INC => '-I/usr/include/mysql',
    LIBS => ['-L/usr/lib -lmysqlclient'],
);

