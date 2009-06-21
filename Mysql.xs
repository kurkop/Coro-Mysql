#include <sys/errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <mysql.h>

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#define IN_DESTRUCT PL_dirty

typedef U16 uint16;

/* cached function gv's */
static CV *readable, *writable;

#include "violite.h"

#define DESC_OFFSET 22

#define CoMy_MAGIC 0x436f4d79

typedef struct {
  int magic;
  SV *corohandle_sv, *corohandle;
  int bufofs, bufcnt;
  char buf[VIO_READ_BUFFER_SIZE];
} ourdata;

#define OURDATAPTR (*((ourdata **)((vio)->desc + DESC_OFFSET)))

static int
our_read (Vio *vio, gptr p, int len)
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

static int
our_write (Vio *vio, const gptr p, int len)
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
          dSP;
          PUSHMARK (SP);
          XPUSHs (OURDATAPTR->corohandle);
          PUTBACK;
          call_sv ((SV *)writable, G_VOID | G_DISCARD);
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
  if (vio->read != our_read)
    croak ("vio.read has unexpected content during unpatch - wtf?");

  if (vio->write != our_write)
    croak ("vio.write has unexpected content during unpatch - wtf?");

  if (vio->vioclose != our_close)
    croak ("vio.vioclose has unexpected content during unpatch - wtf?");

  SvREFCNT_dec (OURDATAPTR->corohandle);
  SvREFCNT_dec (OURDATAPTR->corohandle_sv);

  Safefree (OURDATAPTR);

  vio->read     = vio_read;
  vio->write    = vio_write;
  vio->vioclose = vio_close;

  vio->vioclose (vio);
}

MODULE = Coro::Mysql		PACKAGE = Coro::Mysql

BOOT:
{
  readable = get_cv ("Coro::Mysql::readable", 0);
  writable = get_cv ("Coro::Mysql::writable", 0);
}

PROTOTYPES: ENABLE

void
_patch (IV sock, int fd, SV *corohandle_sv, SV *corohandle)
	CODE:
{
	MYSQL *my = (MYSQL *)sock;
        Vio *vio = my->net.vio;
        ourdata *our;

        if (fd != my->net.fd)
          croak ("DBD::mysql fd and libmysql disagree - library mismatch, unsupported transport or API changes?");

        if (fd != vio->sd)
          croak ("DBD::mysql fd and vio-sd disagree - library mismatch, unsupported transport or API changes?");

        if (vio->vioclose != vio_close)
          croak ("vio.write has unexpected content - library mismatch, unsupported transport or API changes?");

        if (vio->write != vio_write)
          croak ("vio.write has unexpected content - library mismatch, unsupported transport or API changes?");

        if (vio->read != vio_read
            && vio->read != vio_read_buff)
          croak ("vio.read has unexpected content - library mismatch, unsupported transport or API changes?");

        Newz (0, our, 1, ourdata);
        our->magic = CoMy_MAGIC;
        our->corohandle_sv = newSVsv (corohandle_sv);
        our->corohandle    = newSVsv (corohandle);

        vio->desc [DESC_OFFSET - 1] = 0;
        OURDATAPTR = our;

        vio->vioclose = our_close;
        vio->write    = our_write;
        vio->read     = our_read;
}

