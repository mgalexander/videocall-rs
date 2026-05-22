# SFU: drop-slope recovery — trip on "behind", not only "silent" (the IRREVERSIBILITY fix)
Source: `FORWARDING-STALL-ROOTCAUSE.md` (P0 bead 1). After the cliff the room is PERMANENTLY
dark: the vc-9eh watchdog only resubscribes on TOTAL SILENCE (`chat_server.rs:3839`), but a
partially-delivering (starved) subscription is never silent → never trips (acknowledged at
`:3768-3774`). A transient overload kills the room forever.
## Fix: recovery trigger keyed on "behind" — SFU_DISPATCHER_INBOUND_DROPPED_TOTAL rising while
##   receivers present (drop-slope), not just silence. On trip: resubscribe / shed-and-recover so
##   forwarding resumes once offered load drops. Pair with lj-7 (raise cliff) so recovery doesn't
##   thrash. Distinct from lj-3 (this is the dispatcher-drain recovery specifically).
## Acceptance: after an induced overload cliff, forwarding RECOVERS within seconds once load
##   eases (no permanent dark room). P0. code+perf review.
