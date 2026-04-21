# Daily backfill — deployment

`daily_backfill.sh` runs the scrape → DB ingest → retention prune pipeline.
It writes to stdout so the systemd journal (or cron MAILTO) captures logs.

## systemd (recommended)

Assuming the repo lives at `/opt/super-price-il`:

```bash
sudo cp super-price-il.service /etc/systemd/system/
sudo cp super-price-il.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now super-price-il.timer

# one-off smoke test
sudo systemctl start super-price-il.service
journalctl -u super-price-il.service -n 200
```

Edit `WorkingDirectory=` in the `.service` file if the repo isn't at
`/opt/super-price-il`.

## cron (alternative)

```cron
15 4 * * * /opt/super-price-il/scripts/daily_backfill.sh >> /var/log/super-price-il.log 2>&1
```

## What it does

1. Fetches the last 24h of `PriceFull` + `Stores/StoresFull` across the 8
   live chains (`src/cli/backfill.py` default).
2. Prunes `price_observations` + `data/raw/` older than 7 days.
3. Leaves `current_prices` intact (that's the API's fast read path).
