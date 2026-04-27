# AT Command Reference (from `AT+CLAC`)

This reference is derived from the modem's `AT+CLAC` output captured in this project.

It is grouped by function so commands are easier to find during debugging and feature work.
Exact behavior can vary by module firmware; for final semantics, verify with the module AT manual.

## VoLTE Test Quick Set

Most useful commands for VoLTE call test flows:

- `ATD<number>;` dial MO voice call
- `AT+CLCC` read call state list (`active`, `held`, `dialing`, `alerting`, etc.)
- `ATH` / `AT+CHUP` hang up
- `AT+CEER` get release/error reason
- `AT+QNWINFO` RAT/operator context during call
- `AT+CEREG?` and `AT+CGATT?` EPS/packet registration prechecks
- `$QCIMSSRV` IMS service state (vendor-specific)
- `$QCCALLATTR` call attributes (vendor-specific)

## Core Dial Modem Commands

- `A`, `D`, `H`, `O`, `Z`, `T`, `P`
- `E`, `Q`, `V`, `X`
- `&C`, `&D`, `&E`, `&F`, `&S`, `&V`, `&W`
- `\Q`, `\S`, `\V`, `%V`
- `S0`, `S2`-`S11`, `S30`, `S103`, `S104`

## Identity and Device Information

- `+GMI`, `+GMM`, `+GMR`, `+GSN`
- `+CGMI`, `+CGMM`, `+CGMR`, `+CGSN`
- `+GCAP`
- `+ICCID`
- `+QHWV`, `+QUPTIME`
- `$QCHWREV`, `$QCBOOTVER`

## Registration, RAT, and Operator Selection

- `+CREG`, `+CGREG`, `+CEREG`, `+C5GREG`
- `+COPS`
- `+CSQ`, `$CSQ`, `$QCSQ`
- `$QCSYSMODE`, `^SYSINFO`, `^SYSCONFIG`, `^MODE`
- `$QCBANDPREF`, `$QCCOPS`

## Packet Data, PDP, and QoS

- `+CGATT`, `+CGACT`, `+CGDATA`, `+CGCLASS`, `+CGPIAF`
- `+CGDCONT`, `+CGDSCONT`, `+CGTFT`
- `+CGCONTRDP`, `+CGSCONTRDP`, `+CGTFTRDP`
- `+CGQREQ`, `+CGQMIN`, `+CGEQREQ`, `+CGEQMIN`, `+CGEQOS`, `+CGEQOSRDP`
- `+CGPADDR`
- `+C5GNSSAI`, `+C5GNSSAIRDP`
- `+QCFG`, `+QETH`, `+QMAP`, `+QMAPWAC`, `+QMBIM`, `+QIP6CFG`
- `+QDMZ`
- `+QGDCNT?` (supported on this modem; RX/TX counters)

## Voice/Call Supplementary Services

- `+CLCC`, `+CEER`
- `+CHUP`, `+CHLD`
- `+CLIP`, `+COLP`, `+CDIP`, `+CLIR`
- `+CCWA`, `+CCFC`, `+CCUG`, `+CTFR`
- `+VTS`
- `$QCRMCALL`, `$QCCALLATTR`, `$QCCCFC`, `$QCTXDIV` (vendor-specific)

## SIM, Security, and Numbering

- `+CPIN`, `+CLCK`, `+CPWD`
- `+CIMI`, `+CNUM`
- `+CSIM`, `+CRSM`
- `+CPOL`, `+CPLS`, `+COPN`
- `+CPBS`, `+CPBR`, `+CPBF`, `+CPBW`
- `$QCMSISDN`, `$QCPCOMSISDN` (vendor-specific)

## SMS and Messaging

- `+CMGF`, `+CSMS`, `+CGSMS`
- `+CSCA`, `+CSMP`, `+CSDH`, `+CSCB`
- `+CPMS`, `+CNMI`
- `+CMGL`, `+CMGR`, `+CMGS`, `+CMSS`, `+CMGW`, `+CMGD`, `+CMGC`, `+CNMA`, `+CMMS`
- `+C5GSMS`, `+C5GUSMS`

## Time and Clock

- `+CCLK`, `+CTZR`, `+CTZU`
- `$CCLK`, `$CREG` (vendor aliases/extensions)

## Audio and Voice Path

- `+CLVL`, `+CMUT`
- `+QAUDMOD`, `+QAUDCFG`, `+QDAI`, `+QAUDLOOP`
- `+QLDTMF`, `+QLTONE`, `+QTONEDET`
- `+QRXGAIN`, `+QMIC`, `+QSIDET`
- `+QPSND`, `+QAUDRD`, `+QAUDPLAY`, `+QAUDSTOP`, `+QPCMV`, `+QAUDRDY`

## GNSS/Location

- `+QGPS`, `+QGPSEND`, `+QGPSLOC`, `+QGPSCFG`, `+QGPSGNMEA`, `+QGPSDEL`
- `+QGPSXTRA`, `+QGPSXTRATIME`, `+QGPSXTRADATA`
- `+QGPSSUPLURL`, `+QGPSSUPLCA`

## Power, Boot, and Firmware

- `+CFUN`
- `+QPOWD`, `$QCPWRDN`
- `+QSCLK`, `+QSLEEPOUT`
- `+QFASTBOOT`
- `+QUPGRADE`, `+QFOTADL`, `+QFOTAPID`, `+QLWFOTA`
- `+QSECBOOT`, `+QBLKERASE`, `+QNAND`, `+QFTEST`, `+QFCT`, `+QFTCMD`
- `+QTEMP`, `+QTHERMAL`, `+QADC`

## Platform, eSIM, and Vendor Diagnostics

- `+QESIM`
- `+QPLATCFG`, `+QPRFMOD`, `$QCPRFMOD`
- `+QPCIE`, `+QMBIM`
- `+QLOG`, `+QDMSG`, `+QTEST`, `+QCUSTAT`, `+QAGPIO`, `+QWSETMAC`, `+QIIC`, `+QSLIC`
- `$QCCNMI`, `$QCDNSP`, `$QCDNSS`, `$QCTER`
- `$QCSIMSTAT`, `$QCPINSTAT`, `$QCSIMAPP`, `$QCSIMT`
- `$QCNSP`, `$QCRCIND`, `$QCCSGCOPS`
- `$QCPDPP`, `$QCPDPLT`, `$QCPDPCFGE`, `$QCPDPCFGEXT`, `$QCPDPIMSCFGE`
- `$QCAPNE`, `$QCDRX`, `$QCACQDBC`, `$QCANTE`, `$QCRPW`
- `$QCDEFPROF`, `$QCMRUE`, `$QCMRUC`, `$QCPCOLIST`
- `*CNTI`, `^CARDMODE`, `^DSCI`, `^SPN`
- `+CLAC`, `$QCCLAC`

## Notes

- `AT+CLAC` is a capability list, not a full parameter reference.
- Some commands may require SIM state, RAT state, or a live call/data session.
- For unsupported forms (`?`, `=?`), firmware may return `ERROR` even if command appears in `CLAC`.
