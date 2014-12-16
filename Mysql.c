/*
 * This file was generated automatically by ExtUtils::ParseXS version 3.18 from the
 * contents of Mysql.xs. Do not edit this file, edit Mysql.xs instead.
 *
 *    ANY CHANGES MADE HERE WILL BE LOST!
 *
 */

#line 1 "Mysql.xs"
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <mysql.h>

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#if HAVE_EV
# include "EVAPI.h"
# include "CoroAPI.h"
#endif

#define IN_DESTRUCT PL_dirty

typedef U16 uint16;

/* cached function gv's */
static CV *readable, *writable;
static int use_ev;

#include "violite.h"

#define CoMy_MAGIC 0x436f4d79

typedef struct {
#if DESC_IS_PTR
  char desc[30];
  const char *old_desc;
#endif
  int magic;
  SV *corohandle_sv, *corohandle;
  int bufofs, bufcnt;
#if HAVE_EV
  ev_io rw, ww;
#endif
  char buf[VIO_READ_BUFFER_SIZE];
  size_t  (*old_read)(Vio*, uchar *, size_t);
  size_t  (*old_write)(Vio*, const uchar *, size_t);
  int     (*old_vioclose)(Vio*);
} ourdata;

#if DESC_IS_PTR
# define OURDATAPTR (*(ourdata **)&((vio)->desc))
#else
# define DESC_OFFSET 22
# define OURDATAPTR (*((ourdata **)((vio)->desc + DESC_OFFSET)))
#endif

static xlen
our_read (Vio *vio, xgptr p, xlen len)
{
  ourdata *our = OURDATAPTR;

  if (!our->bufcnt)
    {
      int rd;
      my_bool dummy;

      vio->vioblocking (vio, 0, &dummy);

      for (;;)
        {
          rd = recv (vio->sd, our->buf, sizeof (our->buf), 0);

          if (rd >= 0 || errno != EAGAIN)
            break;

#if HAVE_EV
          if (use_ev)
            {
              our->rw.data = (void *)sv_2mortal (SvREFCNT_inc (CORO_CURRENT));
              ev_io_start (EV_DEFAULT_UC, &(our->rw));
              CORO_SCHEDULE;
              ev_io_stop (EV_DEFAULT_UC, &(our->rw)); /* avoids races */
            }
          else
#endif
            {
              dSP;
              PUSHMARK (SP);
              XPUSHs (our->corohandle);
              PUTBACK;
              call_sv ((SV *)readable, G_VOID | G_DISCARD);
            }
        }

      if (rd <= 0)
        return rd;

      our->bufcnt = rd;
      our->bufofs = 0;
    }

  if (our->bufcnt < len)
    len = our->bufcnt;

  memcpy (p, our->buf + our->bufofs, len);
  our->bufofs += len;
  our->bufcnt -= len;

  return len;
}

static xlen
our_write (Vio *vio, cxgptr p, xlen len)
{
  char *ptr = (char *)p;
  my_bool dummy;

  vio->vioblocking (vio, 0, &dummy);

  while (len > 0)
    {
      int wr = send (vio->sd, ptr, len, 0);

      if (wr > 0)
        {
          ptr += wr;
          len -= wr;
        }
      else if (errno == EAGAIN)
        {
          ourdata *our = OURDATAPTR;

#if HAVE_EV
          if (use_ev)
            {
              our->ww.data = (void *)sv_2mortal (SvREFCNT_inc (CORO_CURRENT));
              ev_io_start (EV_DEFAULT_UC, &(our->ww));
              CORO_SCHEDULE;
              ev_io_stop (EV_DEFAULT_UC, &(our->ww)); /* avoids races */
            }
          else
#endif
            {
              dSP;
              PUSHMARK (SP);
              XPUSHs (our->corohandle);
              PUTBACK;
              call_sv ((SV *)writable, G_VOID | G_DISCARD);
            }
        }
      else if (ptr == (char *)p)
        return -1;
      else
        break;
    }

  return ptr - (char *)p;
}

static int
our_close (Vio *vio)
{
  ourdata *our = OURDATAPTR;

  if (vio->read != our_read)
    croak ("vio.read has unexpected content during unpatch - wtf?");

  if (vio->write != our_write)
    croak ("vio.write has unexpected content during unpatch - wtf?");

  if (vio->vioclose != our_close)
    croak ("vio.vioclose has unexpected content during unpatch - wtf?");

#if HAVE_EV
  if (use_ev)
    {
      ev_io_stop (EV_DEFAULT_UC, &(our->rw));
      ev_io_stop (EV_DEFAULT_UC, &(our->ww));
    }
#endif

  SvREFCNT_dec (our->corohandle);
  SvREFCNT_dec (our->corohandle_sv);

#if DESC_IS_PTR
  vio->desc = our->old_desc;
#endif

  vio->vioclose = our->old_vioclose;
  vio->write    = our->old_write;
  vio->read     = our->old_read;

  Safefree (our);

  vio->vioclose (vio);
}

#if HAVE_EV
static void
iocb (EV_P_ ev_io *w, int revents)
{
  ev_io_stop (EV_A, w);
  CORO_READY ((SV *)w->data);
}
#endif

#line 212 "Mysql.c"
#ifndef PERL_UNUSED_VAR
#  define PERL_UNUSED_VAR(var) if (0) var = var
#endif

#ifndef dVAR
#  define dVAR		dNOOP
#endif


/* This stuff is not part of the API! You have been warned. */
#ifndef PERL_VERSION_DECIMAL
#  define PERL_VERSION_DECIMAL(r,v,s) (r*1000000 + v*1000 + s)
#endif
#ifndef PERL_DECIMAL_VERSION
#  define PERL_DECIMAL_VERSION \
	  PERL_VERSION_DECIMAL(PERL_REVISION,PERL_VERSION,PERL_SUBVERSION)
#endif
#ifndef PERL_VERSION_GE
#  define PERL_VERSION_GE(r,v,s) \
	  (PERL_DECIMAL_VERSION >= PERL_VERSION_DECIMAL(r,v,s))
#endif
#ifndef PERL_VERSION_LE
#  define PERL_VERSION_LE(r,v,s) \
	  (PERL_DECIMAL_VERSION <= PERL_VERSION_DECIMAL(r,v,s))
#endif

/* XS_INTERNAL is the explicit static-linkage variant of the default
 * XS macro.
 *
 * XS_EXTERNAL is the same as XS_INTERNAL except it does not include
 * "STATIC", ie. it exports XSUB symbols. You probably don't want that
 * for anything but the BOOT XSUB.
 *
 * See XSUB.h in core!
 */


/* TODO: This might be compatible further back than 5.10.0. */
#if PERL_VERSION_GE(5, 10, 0) && PERL_VERSION_LE(5, 15, 1)
#  undef XS_EXTERNAL
#  undef XS_INTERNAL
#  if defined(__CYGWIN__) && defined(USE_DYNAMIC_LOADING)
#    define XS_EXTERNAL(name) __declspec(dllexport) XSPROTO(name)
#    define XS_INTERNAL(name) STATIC XSPROTO(name)
#  endif
#  if defined(__SYMBIAN32__)
#    define XS_EXTERNAL(name) EXPORT_C XSPROTO(name)
#    define XS_INTERNAL(name) EXPORT_C STATIC XSPROTO(name)
#  endif
#  ifndef XS_EXTERNAL
#    if defined(HASATTRIBUTE_UNUSED) && !defined(__cplusplus)
#      define XS_EXTERNAL(name) void name(pTHX_ CV* cv __attribute__unused__)
#      define XS_INTERNAL(name) STATIC void name(pTHX_ CV* cv __attribute__unused__)
#    else
#      ifdef __cplusplus
#        define XS_EXTERNAL(name) extern "C" XSPROTO(name)
#        define XS_INTERNAL(name) static XSPROTO(name)
#      else
#        define XS_EXTERNAL(name) XSPROTO(name)
#        define XS_INTERNAL(name) STATIC XSPROTO(name)
#      endif
#    endif
#  endif
#endif

/* perl >= 5.10.0 && perl <= 5.15.1 */


/* The XS_EXTERNAL macro is used for functions that must not be static
 * like the boot XSUB of a module. If perl didn't have an XS_EXTERNAL
 * macro defined, the best we can do is assume XS is the same.
 * Dito for XS_INTERNAL.
 */
#ifndef XS_EXTERNAL
#  define XS_EXTERNAL(name) XS(name)
#endif
#ifndef XS_INTERNAL
#  define XS_INTERNAL(name) XS(name)
#endif

/* Now, finally, after all this mess, we want an ExtUtils::ParseXS
 * internal macro that we're free to redefine for varying linkage due
 * to the EXPORT_XSUB_SYMBOLS XS keyword. This is internal, use
 * XS_EXTERNAL(name) or XS_INTERNAL(name) in your code if you need to!
 */

#undef XS_EUPXS
#if defined(PERL_EUPXS_ALWAYS_EXPORT)
#  define XS_EUPXS(name) XS_EXTERNAL(name)
#else
   /* default to internal */
#  define XS_EUPXS(name) XS_INTERNAL(name)
#endif

#ifndef PERL_ARGS_ASSERT_CROAK_XS_USAGE
#define PERL_ARGS_ASSERT_CROAK_XS_USAGE assert(cv); assert(params)

/* prototype to pass -Wmissing-prototypes */
STATIC void
S_croak_xs_usage(pTHX_ const CV *const cv, const char *const params);

STATIC void
S_croak_xs_usage(pTHX_ const CV *const cv, const char *const params)
{
    const GV *const gv = CvGV(cv);

    PERL_ARGS_ASSERT_CROAK_XS_USAGE;

    if (gv) {
        const char *const gvname = GvNAME(gv);
        const HV *const stash = GvSTASH(gv);
        const char *const hvname = stash ? HvNAME(stash) : NULL;

        if (hvname)
            Perl_croak(aTHX_ "Usage: %s::%s(%s)", hvname, gvname, params);
        else
            Perl_croak(aTHX_ "Usage: %s(%s)", gvname, params);
    } else {
        /* Pants. I don't think that it should be possible to get here. */
        Perl_croak(aTHX_ "Usage: CODE(0x%"UVxf")(%s)", PTR2UV(cv), params);
    }
}
#undef  PERL_ARGS_ASSERT_CROAK_XS_USAGE

#ifdef PERL_IMPLICIT_CONTEXT
#define croak_xs_usage(a,b)    S_croak_xs_usage(aTHX_ a,b)
#else
#define croak_xs_usage        S_croak_xs_usage
#endif

#endif

/* NOTE: the prototype of newXSproto() is different in versions of perls,
 * so we define a portable version of newXSproto()
 */
#ifdef newXS_flags
#define newXSproto_portable(name, c_impl, file, proto) newXS_flags(name, c_impl, file, proto, 0)
#else
#define newXSproto_portable(name, c_impl, file, proto) (PL_Sv=(SV*)newXS(name, c_impl, file), sv_setpv(PL_Sv, proto), (CV*)PL_Sv)
#endif /* !defined(newXS_flags) */

#line 354 "Mysql.c"

XS_EUPXS(XS_Coro__Mysql__use_ev); /* prototype to pass -Wmissing-prototypes */
XS_EUPXS(XS_Coro__Mysql__use_ev)
{
    dVAR; dXSARGS;
    if (items != 0)
       croak_xs_usage(cv,  "");
    PERL_UNUSED_VAR(ax); /* -Wall */
    SP -= items;
    {
#line 215 "Mysql.xs"
{
	static int onceonly;

	if (!onceonly)
          {
	    onceonly = 1;
#if HAVE_EV
	    I_EV_API ("Coro::Mysql");
	    I_CORO_API ("Coro::Mysql");
	    use_ev = 1;
#endif
          }

        XPUSHs (use_ev ? &PL_sv_yes : &PL_sv_no);
}
#line 381 "Mysql.c"
	PUTBACK;
	return;
    }
}


XS_EUPXS(XS_Coro__Mysql__patch); /* prototype to pass -Wmissing-prototypes */
XS_EUPXS(XS_Coro__Mysql__patch)
{
    dVAR; dXSARGS;
    if (items != 5)
       croak_xs_usage(cv,  "sock, fd, client_version, corohandle_sv, corohandle");
    {
	IV	sock = (IV)SvIV(ST(0))
;
	int	fd = (int)SvIV(ST(1))
;
	unsigned long	client_version = (unsigned long)SvUV(ST(2))
;
	SV *	corohandle_sv = ST(3)
;
	SV *	corohandle = ST(4)
;
#line 234 "Mysql.xs"
{
	MYSQL *my = (MYSQL *)sock;
        Vio *vio = my->net.vio;
        ourdata *our;

        /* matching versions are required but not sufficient */
        if (client_version != mysql_get_client_version ())
          croak ("DBD::mysql linked against different libmysqlclient library than Coro::Mysql (%lu vs. %lu).",
                 client_version, mysql_get_client_version ());

        if (fd != my->net.fd)
          croak ("DBD::mysql fd and libmysql disagree - library mismatch, unsupported transport or API changes?");

        if (fd != vio->sd)
          croak ("DBD::mysql fd and vio-sd disagree - library mismatch, unsupported transport or API changes?");
#if MYSQL_VERSION_ID < 100010 && !defined(MARIADB_BASE_VERSION)
        if (vio->vioclose != vio_close)
          croak ("vio.vioclose has unexpected content - library mismatch, unsupported transport or API changes?");

        if (vio->write != vio_write)
          croak ("vio.write has unexpected content - library mismatch, unsupported transport or API changes?");

        if (vio->read != vio_read
            && vio->read != vio_read_buff)
          croak ("vio.read has unexpected content - library mismatch, unsupported transport or API changes?");
#endif

        Newz (0, our, 1, ourdata);
        our->magic = CoMy_MAGIC;
        our->corohandle_sv = newSVsv (corohandle_sv);
        our->corohandle    = newSVsv (corohandle);
#if HAVE_EV
        if (use_ev)
          {
            ev_io_init (&(our->rw), iocb, vio->sd, EV_READ);
            ev_io_init (&(our->ww), iocb, vio->sd, EV_WRITE);
          }
#endif
#if DESC_IS_PTR
        our->old_desc = vio->desc;
        strncpy (our->desc, vio->desc, sizeof (our->desc));
        our->desc [sizeof (our->desc) - 1] = 0;
#else
        vio->desc [DESC_OFFSET - 1] = 0;
#endif
        OURDATAPTR = our;

        our->old_vioclose = vio->vioclose;
        our->old_write    = vio->write;
        our->old_read     = vio->read;

        vio->vioclose = our_close;
        vio->write    = our_write;
        vio->read     = our_read;
}
#line 461 "Mysql.c"
    }
    XSRETURN_EMPTY;
}

#ifdef __cplusplus
extern "C"
#endif
XS_EXTERNAL(boot_Coro__Mysql); /* prototype to pass -Wmissing-prototypes */
XS_EXTERNAL(boot_Coro__Mysql)
{
    dVAR; dXSARGS;
#if (PERL_REVISION == 5 && PERL_VERSION < 9)
    char* file = __FILE__;
#else
    const char* file = __FILE__;
#endif

    PERL_UNUSED_VAR(cv); /* -W */
    PERL_UNUSED_VAR(items); /* -W */
#ifdef XS_APIVERSION_BOOTCHECK
    XS_APIVERSION_BOOTCHECK;
#endif
    XS_VERSION_BOOTCHECK;

        (void)newXSproto_portable("Coro::Mysql::_use_ev", XS_Coro__Mysql__use_ev, file, "");
        (void)newXSproto_portable("Coro::Mysql::_patch", XS_Coro__Mysql__patch, file, "$$$$$");

    /* Initialisation Section */

#line 205 "Mysql.xs"
{
  readable = get_cv ("Coro::Mysql::readable", 0);
  writable = get_cv ("Coro::Mysql::writable", 0);
}

#line 497 "Mysql.c"

    /* End of Initialisation Section */

#if (PERL_REVISION == 5 && PERL_VERSION >= 9)
  if (PL_unitcheckav)
       call_list(PL_scopestack_ix, PL_unitcheckav);
#endif
    XSRETURN_YES;
}
