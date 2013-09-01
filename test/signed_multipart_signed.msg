Date: Mon, 27 Feb 2012 10:12:07 -0800
From: Steve Langasek <vorlon@debian.org>
To: debian-devel@lists.debian.org
Subject: Re: upstart: please update to latest upstream version
Message-ID: <20120227181207.GC11408@virgil.dodds.net>
Mail-Followup-To: debian-devel@lists.debian.org
References: <20120221130641.8560771dm371on9t@webmail.in-berlin.de>
 <4F43E10A.2050309@debian.org>
 <8FDF8775-E1F2-45B3-853D-D01E3DD129F9@physik.fu-berlin.de>
 <20120221195912.GA2132@virgil.dodds.net>
 <58360B23-DC16-45A0-966D-CB7A49588032@physik.fu-berlin.de>
 <20120221211208.GB2132@virgil.dodds.net>
 <20120222132447.GA6277@afflict.kos.to>
 <20120224094759.GB13537@virgil.dodds.net>
 <20120227102425.GA28094@afflict.kos.to>
MIME-Version: 1.0
Content-Type: multipart/signed; micalg=pgp-sha256;
     protocol="application/pgp-signature"; boundary="da4uJneut+ArUgXk"
Content-Disposition: inline
In-Reply-To: <20120227102425.GA28094@afflict.kos.to>
User-Agent: Mutt/1.5.21 (2010-09-15)


--da4uJneut+ArUgXk
Content-Type: text/plain; charset=us-ascii
Content-Disposition: inline
Content-Transfer-Encoding: quoted-printable

On Mon, Feb 27, 2012 at 12:24:25PM +0200, Riku Voipio wrote:
> On Fri, Feb 24, 2012 at 01:47:59AM -0800, Steve Langasek wrote:
> > > I have. Not on debian, but on debianish system with dash. And the res=
ult
> > > was that shellscripts are indeed the bottleneck.  We still did conver=
t to
> > > upstart since we believed it would allow us to cut down the amount of
> > > shell scripts.  The event based architecture however forced much more
> > > shell scripting[1] that made the boot time improvement much less than
> > > hoped.

> > Interesting to know, thanks.  Was this done in a head-to-head comparison
> > with the systemd "no shell" boot?

> Systemd was not at ready that time.. Every exec has a price, and if a
> upstart stanza has a pre-start and post-start script, that is already two
> shell execs.  Any command in shell script that is not a shell builtin is
> more execs.  In this sense dash is worse, as it has less builtins that
> bash.  for example sleep is not a builtin...

No disagreement with any of that.  But how high is that price, in point of
fact?  If no one's measured it, then converting scripts to C programs to
avoid the added exec() calls is premature optimization.  If the only
comparison is between two different distros, you might find that your boot
time is higher for completely unrelated reasons (bug in a video driver?
handling of corner cases in one path but not the other, that impact the
reliability for that 1% of users it matters to?).

I would certainly welcome it if someone did profiling that showed whether
the shells are a bottleneck.  My own subjective experience is that this is
probably not the low-hanging fruit on a general-purpose distro, but if it
turns out that there are significant speed improvements to be had, we could
certainly look at moving more core startup functionality into C for upstart.

> The other factor of shell scripts is psychological. Since shell scripts a=
re
> so easy to modify, people tend to litter them with unneccesary checks,
> settings, workarounds and other spagethi.=20

One of the worst contributors to the use of 'script' in upstart jobs instead
of 'exec' is the need for backwards-compatibility with pre-upstart
/etc/default/* files.  The options here are all fairly poor:

 - ignore the admin's /etc/default settings when switching init systems
 - migrate any local changes to /etc/default into the upstart job at upgrade
   time, by editing a conffile in a maintainer script
 - keep sourcing /etc/default at runtime

I guess systemd has largely chosen option 1 (in part because there's a weird
view in the systemd community that these jobs belong upstream, so Debian
integration issues are entirely ignored).  For many upstart jobs in Ubuntu,
we've chosen option 3.  Which do you think is the right solution?  Are there
other options I haven't seen?

> > If you have other stuff *like* this, I'd be interested to see it.  That
> > particular upstart job is needed only because of peculiarities of how c=
ups
> > starts up.
>=20
> Well that was a common development idiom with upstart when our developers
> realized that neither "on started" or "on starting" did what they
> wanted...  Some of them were got rid of by changing upstream code to emit
> a signal when really ready.

Right, this requires a definition of "started" that's appropriate for the
service in question.  Socket-based activation would be another solution
here, but that might also require changing upstream code anyway.

FWIW, aside from this one bug affecting cups, I'm pretty sure that *any*
service for which upstart's "on started" is broken/racy is also broken/racy
in sysvinit.  The code really ought to be fixed upstream for everyone's
benefit.

--=20
Steve Langasek                   Give me a lever long enough and a Free OS
Debian Developer                   to set it on, and I can move the world.
Ubuntu Developer                                    http://www.debian.org/
slangasek@ubuntu.com                                     vorlon@debian.org

--da4uJneut+ArUgXk
Content-Type: application/pgp-signature; name="signature.asc"
Content-Description: Digital signature

-----BEGIN PGP SIGNATURE-----
Version: GnuPG v1.4.11 (GNU/Linux)

iQIVAwUBT0vHd1aNMPMhshM9AQjvDA/+ND8KAY6Dhcb9XGloEcCNqZn6QmuU/Mrk
giTJW9mpkcOcIGZ+AWbakNrbTv4QlFV4CPJ2Qb9HivlkJQ16QmKgrO3IRprZYA9l
fFQbifgII3qCR0trEnqWoHSJeH9POwuysWmhSWSYMn01DcLLECPabQWeCYBzvlVN
Pt2Jlj0n9+8aTPhQjy/cN1LF6vWc7uU7bBxWHPvXzWEESxAzpZc0TQNk2eXCMkxm
Hp5rx2HWuBVFipkFvxR8wZb5hmSFKNlbBMgtnckibPP0BGLkNstOeRp5uUyQ2sN4
1sUBNyNQyccwy17ei81/rRgH0DDFmcVJ+9n2bB560QqelmEwsyZV4ptseqzWLogx
qTs6FZfR1cNBHxwIzaXVNL8OvmH/qJ/cd1pGtUByHSVfkVKVj/IzvKMXdR0dWoWk
Dhdz1Rr2tBOpbE4wu6JPjgT8iIARI5nmtBqgsZmEyJUOMlhl7+0q3KnRtp0a8HS3
viS5oBdOfHSBBfa4+sGbioh7Tjxk1wcfB0kkTaV3ao4tA2Pj5qSm7eqUUB18uXqB
jde0IWpPfxzTkIO+dPyY+ehYUW+tQWsqwx3rDFnZgUutEV6EBBqO5ay1QRY81mYw
NICQ7ooCRa/QzTqnXkZXf/uiwsgaNd1znj85MEA3UapLQliV3a3niRIT3kWwAUpZ
AZpswChW+c0=
=S7II
-----END PGP SIGNATURE-----

--da4uJneut+ArUgXk--


-- 
To UNSUBSCRIBE, email to debian-devel-REQUEST@lists.debian.org
with a subject of "unsubscribe". Trouble? Contact listmaster@lists.debian.org
Archive: http://lists.debian.org/20120227181207.GC11408@virgil.dodds.net

