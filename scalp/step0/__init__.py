"""Step 0 — discovery scripts.

These exist to be run once, by hand, in order, before any pipeline code is
written. They answer questions the design depends on and that cannot be
answered by reading documentation:

    s0_availability.py     Is trade_quote on the Standard subscription?  GATE.
    s1_venue_check.py      Which endpoints actually need venue=utp_cta?
    s2_fdx_one_day.py      Row count, real schema, wall clock, parquet size.
    s3_multiday_timing.py  Does a 544-symbol backfill need concurrency?
    s4_conditions.py       Which trade condition codes to exclude.
    s5_quote_emission.py   Are quotes emitted on change only or every update?

RUN s0 ALONE FIRST AND STOP. If trade_quote is not on the subscription,
nothing else in this directory is worth running.

Nothing here writes to Postgres, and nothing writes outside config.STEP0_DIR.
"""
