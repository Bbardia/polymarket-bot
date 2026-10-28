# AWC one-shot recurrence (review-only; not installed)

The public-only runner is `python -m src.v3.awc_campaign_runner`. It uses the **same collector** (`DailyCampaign`) and exact daily Gamma/rule/market validation; it does not trade, read funded state, carry credentials, or create a forecast issue time. It always confines output to the repository's approved `data/awc-campaign/recurring/` root: immutable raw tape entries under `tape/`, journal entries under `journal/`, and an advisory singleton `.lock`. Existing manual entries in `data/awc-campaign/*.json` are not read or modified. Do not use the old manual tape as input to this new campaign.

## Operator template, not a scheduled service

After independent review and a separate launch authorization, pin a **current UTC instant** once and use the identical literal `--start-at` on every invocation; do not backdate it. From the reviewed checkout (substitute its absolute path and a compatible Python interpreter):

```sh
<ABSOLUTE_REVIEWED_PYTHON> -m src.v3.awc_campaign_runner --start-at=<PINNED_CURRENT_UTC_ISO8601>
<ABSOLUTE_REVIEWED_PYTHON> -m src.v3.awc_campaign --config <REVIEWED_EVENT_CONFIG> --directory data/awc-campaign/recurring/tape --report
```

The second command is a read-only tape report; an explicit config is required by that CLI even for `--report`. The runner itself has no scheduler. A **future, separately reviewed** systemd user timer should invoke the first command at `08:00`, `10:00`, `14:00`, and `20:00` **Asia/Karachi**, with `Persistent=false`, no catch-up, no shell interpolation, and a single pinned start argument. The 10:00 slot selects an eligible captured but not same-venue-corroborated prior Karachi date, preferring the fewest prior 10:00 STARTs and then the oldest date. Each date receives at most three such revisits; a still-pending day beyond that cap is not silently counted as resolved. There is a ten-minute start tolerance, not a request for historical snapshots. Start times before the pinned instant are skipped. A manual invocation outside a window performs no GET; after an outage, eligible past slots are durable `missed` entries without GET. An invocation may take up to the collector's 60-second cycle wall budget plus bounded local journal/tape replay; do not configure an overlapping timer as a second collector. The flock prevents overlap.

**Illustrative only; do not install/enable without separate review** (replace every angle-bracket placeholder; no credentials or inherited proxy settings):

```ini
# awc-paper.service (proposed)
[Unit]
Description=Public-only AWC paper evidence one-shot
[Service]
Type=oneshot
WorkingDirectory=<ABSOLUTE_REVIEWED_CHECKOUT>
ExecStart=<ABSOLUTE_REVIEWED_PYTHON> -m src.v3.awc_campaign_runner --start-at=<PINNED_CURRENT_UTC_ISO8601>
Environment=PYTHONUNBUFFERED=1
```

```ini
# awc-paper.timer (proposed)
[Unit]
Description=AWC paper slots in Karachi local time
[Timer]
OnCalendar=*-*-* 08,10,14,20:00:00 Asia/Karachi
Persistent=false
AccuracySec=1s
RandomizedDelaySec=0
Unit=awc-paper.service
[Install]
WantedBy=timers.target
```

Before deployment, verify the target systemd version accepts that calendar expression with `systemd-analyze calendar`, isolate the service environment, set a bounded service timeout that includes replay, and separately approve activation. The timer is **not** installed by this change.

## Durable accounting

A `started` entry is fsynced before **any** public GET. A `finished` entry uses a fixed code and never includes an exception or provider body. `missed` slots count toward the 180-attempt budget but perform no GET. A crash after `started`, including after successful tape publication but before `finished`, consumes its slot permanently. No automatic retry, even after a failed Gamma lookup, missing rule, timeout, or signal. Entries are small canonical hash-chained files, published exclusively after file fsync and followed by directory fsync; readers reject symlinks, oversize, bad sequence, hash drift, changed start, duplicate slots and invalid transitions. A separate no-follow lock serializes timer and manual invocations. Do not delete/repair a started-only entry to make another GET happen.

The runner records a terminal `stop` and never fetches again at the first of **30 unique venue-corroborated resolved event-days**, **45 Karachi calendar dates** beginning with the pinned local start date (the 46th date is excluded), or **180 attempted/missed slots**. Success is a collector capture, **not** settlement: only the existing tape replay's same-venue Gamma + Data API validation contributes a resolved day. A failed API, partial AWC capture, and collector refusal cannot become finality. Journal and tape are replayed within finite row/byte limits; a corrupt/oversized row aborts before network. This is not independent on-chain UMA finality or proof of AWC correction chronology. Forecast run issuance remains unknown. All rules and initial daily target selection still require external review.

## Launch gates

- Independently review the daily Gamma rule and selected rung, candidate manifest/condition/tokens, and code diff; first-seen API discovery is not independent authorization.
- Review the systemd unit/timer/env isolation and timezone on the target host separately; **none is installed or enabled here**. Keep funded service and checkout untouched. Ensure a network-isolated temporary smoke invocation before any real capture.
- Establish real current UTC `--start-at`, compatible interpreter, writable approved research directory, and operational monitoring for missed slots, interrupted STARTs, unexpected fixed failure codes, and terminal stop. No replay of missed time windows.
- The collector is limited to seven public GETs (including exact YES and NO books), source-specific byte caps, no redirects/proxies/ambient credentials, and a 60-second interrupting cycle deadline. An absent NO ask is unpriced, not a paper fill. Those limits do not imply historical forecast issuance, independent resolution, or trading eligibility.
