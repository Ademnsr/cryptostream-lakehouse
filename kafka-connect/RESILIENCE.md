# Kafka Connect offset-recovery test

Killed the S3 sink connector mid-stream twice (once on purpose, once by
accident) while the producer kept streaming real trades, to check it
doesn't lose or duplicate data on restart.

## Why it's safe

The S3 sink only commits a Kafka offset after a batch is actually flushed
to S3. If it dies before that, the offset just doesn't move, and it
re-reads the same records on restart. Topic retention is 3 days, so
there's plenty of room to re-read.

## Test 1: `docker kill`

Offsets before the kill: partition 0 = 460, partition 2 = 403.

```
docker kill kafka-connect
```

Producer kept writing to Kafka the whole time (partition 0 kept climbing,
492 → 556, while Connect was down). Restarted the container, and the logs
show it picked back up at the exact same offsets:

```
Setting offset for partition crypto.market-trades.raw.v1-0 to the committed offset ... offset=460
Setting offset for partition crypto.market-trades.raw.v1-2 to the committed offset ... offset=403
```

## Test 2: real crash

While waiting to check the next S3 flush, the host actually lost network.
The task died with `UnknownHostException: s3.eu-west-3.amazonaws.com`
(connector is set to `errors.tolerance: none`, so it fails hard instead of
retrying silently forever). The producer's websocket dropped around the
same time. Offsets were still 460 / 403 — nothing had been lost. Restarted
the task via the REST API and it resumed from the same offsets again.

## Result

Ran for ~15 more minutes with normal flushes after that, offsets climbing
as expected (460 → 3419, 403 → 2340), no errors. Two crashes, same
recovery point both times: no data loss, and no offset skipped ahead of
what was actually saved in S3.
