//! Optional SQLite persistence (feature `storage`).
//!
//! Events are stored with their raw body retained, so unknown event types are
//! never lost and can be decoded later. A per-device sync cursor enables
//! incremental syncs. Re-syncing is idempotent: identical events are de-duped.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::Result;
use oura_protocol::device::{Battery, DeviceInfo};
use oura_protocol::events::RingEvent;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS device (
    serial        TEXT PRIMARY KEY,
    hardware_id   TEXT,
    firmware      TEXT,
    api_version   TEXT,
    mac           TEXT,
    updated_unix  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    serial        TEXT PRIMARY KEY,
    next_cursor   INTEGER NOT NULL,
    last_sync_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    serial         TEXT NOT NULL,
    tag            INTEGER NOT NULL,
    name           TEXT NOT NULL,
    ring_timestamp INTEGER NOT NULL,
    body           BLOB NOT NULL,
    decoded_json   TEXT,
    captured_unix  INTEGER NOT NULL,
    UNIQUE(serial, tag, ring_timestamp, body)
);
CREATE INDEX IF NOT EXISTS idx_events_serial_tag ON events(serial, tag);
CREATE INDEX IF NOT EXISTS idx_events_capture ON events(captured_unix, id);
CREATE INDEX IF NOT EXISTS idx_events_tag_time ON events(tag, ring_timestamp);

CREATE TABLE IF NOT EXISTS readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    serial        TEXT NOT NULL,
    kind          TEXT NOT NULL,
    value         REAL NOT NULL,
    unit          TEXT,
    captured_unix INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_serial_kind ON readings(serial, kind);
"#;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A SQLite-backed store for ring data.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open (creating if needed) a database at `path` and ensure the schema.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let conn = Connection::open(path)?;
        // Health data + device identifiers are sensitive; keep the DB owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| crate::error::Error::Storage(e.to_string()))?;
        }
        conn.busy_timeout(std::time::Duration::from_millis(5000))?;
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if mode != "wal" {
            return Err(crate::error::Error::Storage(format!(
                "WAL unavailable: {mode}"
            )));
        }
        conn.execute_batch("PRAGMA synchronous=FULL;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Read without changing schema, permissions, or journal mode (including bundled seeds).
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(std::time::Duration::from_millis(5000))?;
        Ok(Self { conn })
    }

    /// Commit a complete protocol batch and its cursor together, before ACK/progress.
    pub fn commit_batch(&self, serial: &str, events: &[RingEvent], cursor: u32) -> Result<u32> {
        let tx = self.conn.unchecked_transaction()?;
        let mut inserted = 0;
        for event in events {
            inserted += u32::from(self.insert_event(serial, event)?);
        }
        self.set_cursor(serial, cursor)?;
        tx.commit()?;
        Ok(inserted)
    }

    /// Write a self-contained copy of the database (WAL folded in) to `out_path`,
    /// for sharing a phone's raw ring records with the desktop tooling. Any file
    /// at `out_path` is replaced.
    pub fn export_to<P: AsRef<Path>>(&self, out_path: P) -> Result<()> {
        let out = out_path.as_ref();
        if out.exists() {
            std::fs::remove_file(out).map_err(|e| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                    Some(format!("remove {}: {e}", out.display())),
                )
            })?;
        }
        let path = out.to_string_lossy().into_owned();
        self.conn.execute("VACUUM INTO ?1", params![path])?;
        Ok(())
    }

    pub fn integrity_check(&self) -> Result<String> {
        Ok(self
            .conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))?)
    }

    /// Open an in-memory database (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Record/refresh device metadata.
    pub fn upsert_device(
        &self,
        serial: &str,
        hardware_id: Option<&str>,
        info: Option<&DeviceInfo>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO device (serial, hardware_id, firmware, api_version, mac, updated_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(serial) DO UPDATE SET
               hardware_id=COALESCE(excluded.hardware_id, device.hardware_id),
               firmware=COALESCE(excluded.firmware, device.firmware),
               api_version=COALESCE(excluded.api_version, device.api_version),
               mac=COALESCE(excluded.mac, device.mac),
               updated_unix=excluded.updated_unix",
            params![
                serial,
                hardware_id,
                info.map(|i| i.firmware_version.clone()),
                info.map(|i| i.api_version.clone()),
                info.map(|i| i.mac.clone()),
                now_unix(),
            ],
        )?;
        Ok(())
    }

    /// Device identity + last-sync for display: the most-recently-updated device
    /// row joined with its sync state.
    /// Returns `(serial, hardware_id, firmware, api_version, mac, updated_unix, last_sync_unix, next_cursor)`.
    #[allow(clippy::type_complexity)]
    pub fn device_info(
        &self,
    ) -> Result<Option<(String, String, String, String, String, i64, i64, i64)>> {
        let row = self
            .conn
            .query_row(
                "SELECT d.serial, COALESCE(d.hardware_id,''), COALESCE(d.firmware,''),
                        COALESCE(d.api_version,''), COALESCE(d.mac,''), COALESCE(d.updated_unix,0),
                        COALESCE(s.last_sync_unix,0), COALESCE(s.next_cursor,0)
                 FROM device d LEFT JOIN sync_state s ON s.serial = d.serial
                 ORDER BY d.updated_unix DESC LIMIT 1",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                        r.get::<_, i64>(6)?,
                        r.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;
        Ok(row)
    }

    /// The persisted incremental-sync cursor (deciseconds), or 0 if none.
    /// Whether any event is already stored for `serial`. Cheap (index-only
    /// existence check) and deliberately not a count: callers want to know
    /// "has this ring ever been drained into this database", to tell a genuine
    /// first sync apart from a populated one.
    pub fn has_events(&self, serial: &str) -> Result<bool> {
        let present: i64 = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE serial = ?1)",
            params![serial],
            |r| r.get(0),
        )?;
        Ok(present != 0)
    }

    /// Total events stored for `serial` and the newest ring timestamp among
    /// them. Reported before a drain starts so a database that lost its history
    /// — an app container wiped by a reinstall, say — is visible immediately
    /// rather than inferred hours later from a sync that re-pulls everything.
    pub fn event_stats(&self, serial: &str) -> Result<(i64, i64)> {
        self.conn
            .query_row(
                "SELECT COUNT(*), COALESCE(MAX(ring_timestamp), 0)
                   FROM events WHERE serial = ?1",
                params![serial],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(Into::into)
    }

    pub fn cursor(&self, serial: &str) -> Result<u32> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT next_cursor FROM sync_state WHERE serial = ?1",
                params![serial],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.unwrap_or(0) as u32)
    }

    /// Persist the next sync cursor.
    pub fn set_cursor(&self, serial: &str, cursor: u32) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_state (serial, next_cursor, last_sync_unix)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(serial) DO UPDATE SET
               next_cursor=excluded.next_cursor,
               last_sync_unix=excluded.last_sync_unix",
            params![serial, cursor as i64, now_unix()],
        )?;
        Ok(())
    }

    /// Insert an event, ignoring exact duplicates. Returns true if a row was added.
    pub fn insert_event(&self, serial: &str, ev: &RingEvent) -> Result<bool> {
        let decoded = ev
            .decoded
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default());
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO events
               (serial, tag, name, ring_timestamp, body, decoded_json, captured_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                serial,
                ev.tag as i64,
                ev.name,
                ev.timestamp as i64,
                ev.body,
                decoded,
                now_unix(),
            ],
        )?;
        Ok(changed > 0)
    }

    /// Insert an event with an explicit capture time instead of now. Used when a
    /// row is derived from evidence captured earlier, so downstream epoch
    /// selection (which keys on capture time) treats it as of that moment.
    pub fn insert_event_captured_at(
        &self,
        serial: &str,
        ev: &RingEvent,
        captured_unix: i64,
    ) -> Result<bool> {
        let decoded = ev
            .decoded
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default());
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO events
               (serial, tag, name, ring_timestamp, body, decoded_json, captured_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                serial,
                ev.tag as i64,
                ev.name,
                ev.timestamp as i64,
                ev.body,
                decoded,
                captured_unix,
            ],
        )?;
        Ok(changed > 0)
    }

    /// Distil sync exhaust into time anchors, then delete it.
    ///
    /// A Gen 3 ring answers every data-flush command by logging a fixed group of
    /// records — `check_sleep`, `s: <ds>`, `e: <ds>`, `not needed`, plus a few
    /// `debug_data` — into the very history buffer being drained. A drain that
    /// flushed per batch and never terminated left over a million of them. They
    /// are worthless as data but priceless as evidence: each was generated and
    /// downloaded within a second, so its `(ring_timestamp, captured_unix)` pair
    /// is a wall-clock anchor — dense through periods where the ring's own
    /// counter was running away or frozen and nothing else can date the events
    /// around it.
    ///
    /// Only the `debug_event` and `ble_connection` records go. The `debug_data`
    /// (0x61) records a flush produces are kept: the summary reads them as
    /// nocturnal support signals, and deleting them from the edges of a night
    /// made the night disappear. They are small.
    ///
    /// The ring also logs the same group on its own, every twenty minutes or so.
    /// Exhaust is told apart by structure, not content: flush groups from a
    /// draining sync sit a few deciseconds apart, organic ones hundreds at the
    /// least, so a run of `EXHAUST_MIN_GROUPS` or more `check_sleep` groups each
    /// within `EXHAUST_GROUP_GAP_DS` of the previous is exhaust, and every log
    /// record inside the run's span goes with it. Anchors come only from runs
    /// that were plainly fetched live — at least `LIVE_S_PER_GROUP` of download
    /// time per group; a stale block fetched in bulk by a later sync arrives
    /// hundreds of groups a second and would claim its download time as its
    /// generation time — thinned to one per `ANCHOR_SPACING_S` of capture time
    /// and written as phone-sourced `time_sync` (0x42) rows dated to the original
    /// capture. Returns `(anchors_written, rows_deleted)`.
    pub fn compact_exhaust(&self, serial: &str) -> Result<(u32, u64)> {
        const EXHAUST_GROUP_GAP_DS: i64 = 60;
        const EXHAUST_MIN_GROUPS: usize = 3;
        const LIVE_S_PER_GROUP: f64 = 0.1;
        const STALE_SLACK_S: i64 = 5;
        const LIVE_CHAIN_GAP_S: i64 = 30;
        const ANCHOR_SPACING_S: i64 = 30;

        let groups: Vec<(i64, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT ring_timestamp, captured_unix FROM events
                  WHERE serial = ?1 AND tag = 0x43 AND decoded_json LIKE '%check_sleep%'
                  ORDER BY ring_timestamp, id",
            )?;
            let rows = stmt
                .query_map(params![serial], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        // Runs of closely spaced groups: (first ds, last ds, live groups, live
        // capture window). The window is the chain of groups each fetched within
        // LIVE_CHAIN_GAP_S of the previous: a live drain fetches a group every few
        // hundred milliseconds, so the first group fetched hours after its
        // predecessor — the tail of a run the app stopped draining, picked up by a
        // later sync — ends the window. Everything in the run is still deleted.
        let mut runs: Vec<(i64, i64, usize, (i64, i64))> = Vec::new();
        let mut run: Vec<(i64, i64)> = Vec::new();
        let mut flush_run = |run: &mut Vec<(i64, i64)>, runs: &mut Vec<(i64, i64, usize, (i64, i64))>| {
            if run.len() >= EXHAUST_MIN_GROUPS {
                let first = run.first().unwrap().0;
                let last = run.last().unwrap().0;
                let cmin = run[0].1;
                let mut wend = cmin;
                let mut live_groups = 0usize;
                for &(_, captured) in run.iter() {
                    if captured - wend > LIVE_CHAIN_GAP_S {
                        break;
                    }
                    wend = wend.max(captured);
                    live_groups += 1;
                }
                runs.push((first, last, live_groups, (cmin, wend)));
            }
            run.clear();
        };
        for &(ds, captured) in &groups {
            if run.last().is_some_and(|&(prev, _)| ds - prev > EXHAUST_GROUP_GAP_DS) {
                flush_run(&mut run, &mut runs);
            }
            run.push((ds, captured));
        }
        flush_run(&mut run, &mut runs);
        if runs.is_empty() {
            return Ok((0, 0));
        }

        // The ring's own anchors are authoritative. A candidate that would put the
        // counter out of order with one of them — a smaller ds at a later time, or
        // the reverse — was fetched long after it was generated, whatever its run
        // looked like, and must not become an anchor.
        let ring_anchors: Vec<(i64, i64)> = {
            let mut stmt = self.conn.prepare(
                "SELECT ring_timestamp, json_extract(decoded_json, '$.unix_time')
                   FROM events
                  WHERE serial = ?1 AND tag IN (0x42, 0x85)
                    AND COALESCE(json_extract(decoded_json, '$.source'), 'ring') <> 'phone'
                    AND json_extract(decoded_json, '$.unix_time') IS NOT NULL",
            )?;
            let rows = stmt
                .query_map(params![serial], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        let consistent_with_ring = |ds: i64, unix: i64| {
            ring_anchors
                .iter()
                .all(|&(rds, runix)| !((rds > ds && runix < unix) || (rds < ds && runix > unix)))
        };

        let tx = self.conn.unchecked_transaction()?;
        let mut anchors = 0u32;
        let mut deleted = 0u64;
        for &(first, last, count, captured_span) in &runs {
            let span_s = captured_span.1 - captured_span.0;
            // The last group's own records trail it by a few deciseconds.
            let end = last + EXHAUST_GROUP_GAP_DS;
            if span_s as f64 >= LIVE_S_PER_GROUP * count as f64 {
                let mut stmt = self.conn.prepare(
                    "SELECT ring_timestamp, captured_unix FROM events
                      WHERE serial = ?1 AND tag IN (0x43, 0x5b)
                        AND ring_timestamp BETWEEN ?2 AND ?3
                      ORDER BY captured_unix, id",
                )?;
                let live = stmt
                    .query_map(params![serial, first, end], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                // Only rows fetched during the run itself. A flush's records can
                // outlive the sync that caused them — the app is backgrounded, the
                // link drops — and be fetched hours later by the next sync; inside a
                // live run they are structurally exhaust and go, but an anchor made
                // from one would pin the ring's counter to the wrong time of day.
                let (cmin, cmax) = (captured_span.0, captured_span.1);
                let mut last_anchor_capture: Option<i64> = None;
                for (ds, captured) in live {
                    if captured < cmin || captured > cmax + STALE_SLACK_S {
                        continue;
                    }
                    if !consistent_with_ring(ds, captured) {
                        continue;
                    }
                    if last_anchor_capture.is_some_and(|l| captured - l < ANCHOR_SPACING_S) {
                        continue;
                    }
                    let unix = u32::try_from(captured).unwrap_or(0);
                    let mut body = unix.to_le_bytes().to_vec();
                    body.extend_from_slice(b"phone");
                    let anchor = RingEvent {
                        tag: 0x42,
                        name: oura_protocol::events::event_name(0x42),
                        timestamp: u32::try_from(ds).unwrap_or(0),
                        body,
                        decoded: Some(serde_json::json!({ "unix_time": unix, "source": "phone" })),
                    };
                    if self.insert_event_captured_at(serial, &anchor, captured)? {
                        anchors += 1;
                    }
                    last_anchor_capture = Some(captured);
                }
            }
            deleted += self.conn.execute(
                "DELETE FROM events WHERE serial = ?1 AND tag IN (0x43, 0x5b)
                    AND ring_timestamp BETWEEN ?2 AND ?3",
                params![serial, first, end],
            )? as u64;
        }
        tx.commit()?;
        Ok((anchors, deleted))
    }

    /// Reclaim the space freed by [`Self::compact_exhaust`]. Separate because it
    /// rewrites the whole file: callers run it only when enough was deleted.
    pub fn vacuum(&self) -> Result<()> {
        self.conn.execute_batch("VACUUM;")?;
        Ok(())
    }

    /// Record a scalar reading (e.g. live HR bpm, SpO2 %, battery %).
    pub fn insert_reading(&self, serial: &str, kind: &str, value: f64, unit: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO readings (serial, kind, value, unit, captured_unix)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![serial, kind, value, unit, now_unix()],
        )?;
        Ok(())
    }

    /// Convenience: store a battery reading.
    pub fn insert_battery(&self, serial: &str, battery: &Battery) -> Result<()> {
        self.insert_reading(serial, "battery_percent", battery.percent as f64, "%")
    }

    /// Re-decode every stored event body with the current decoders, updating
    /// `decoded_json`. Returns `(rows_with_decode, total_rows)`. Lets new decoders
    /// be applied to events captured before they existed — no re-sync needed.
    pub fn redecode(&self) -> Result<(usize, usize)> {
        let rows: Vec<(i64, i64, Vec<u8>)> = {
            let mut stmt = self.conn.prepare("SELECT id, tag, body FROM events")?;
            let collected = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            collected
        };
        let total = rows.len();
        let mut decoded_count = 0;
        for (id, tag, body) in rows {
            let decoded = oura_protocol::events::decode_event_body(tag as u8, &body)
                .map(|v| serde_json::to_string(&v).unwrap_or_default());
            if decoded.is_some() {
                decoded_count += 1;
            }
            let name = oura_protocol::events::event_name(tag as u8);
            self.conn.execute(
                "UPDATE events SET decoded_json = ?1, name = ?2 WHERE id = ?3",
                params![decoded, name, id],
            )?;
        }
        Ok((decoded_count, total))
    }

    /// All decoded events as `(ring_timestamp_deciseconds, tag, decoded_json,
    /// captured_unix)`, ordered by ring time. For analysis/reporting commands that
    /// reconstruct time series from stored events.
    pub fn decoded_events(&self) -> Result<Vec<(i64, u8, String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ring_timestamp, tag, decoded_json, captured_unix FROM events \
             WHERE decoded_json IS NOT NULL ORDER BY captured_unix, id",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)? as u8,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Distinct device serials that have stored events.
    pub fn device_serials(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT serial FROM events ORDER BY serial")?;
        let rows = stmt
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Count stored events grouped by event name (descending).
    pub fn event_counts(&self, serial: &str) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, COUNT(*) FROM events WHERE serial = ?1 GROUP BY name ORDER BY 2 DESC",
        )?;
        let rows = stmt
            .query_map(params![serial], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> RingEvent {
        RingEvent {
            tag: 0x43,
            name: "debug_event",
            timestamp: 42,
            body: vec![1, 2, 3],
            decoded: None,
        }
    }
    #[test]
    fn compact_exhaust_deletes_flush_runs_and_anchors_only_live_ones() {
        use oura_protocol::events::RingEvent;
        let store = Store::open_in_memory().unwrap();
        let log = |tag: u8, ts: u32, text: &str| RingEvent {
            tag,
            name: "debug".into(),
            timestamp: ts,
            body: text.as_bytes().to_vec(),
            decoded: Some(serde_json::json!({ "ascii": text })),
        };
        let flush_group = |ts: u32| {
            vec![
                log(0x43, ts, "check_sleep"),
                log(0x43, ts + 1, "s: 6404585"),
                log(0x43, ts + 2, "e: 6695538"),
                log(0x43, ts + 3, "not needed"),
                log(0x61, ts + 4, "{\"_status\":\"unvalidated\"}"),
                log(0x61, ts + 5, "{\"_status\":\"unvalidated\"}"),
            ]
        };
        // Organic: the ring's own check_sleep group every 240 ds through a frozen
        // night, bulk-downloaded later in one second. Dense-ish, widely spaced.
        for i in 0..30u32 {
            for ev in flush_group(1_000 + i * 240) {
                store.insert_event_captured_at("S1", &ev, 2_000_000).unwrap();
            }
        }
        // Live exhaust: 200 flush groups 9 ds apart, five groups per captured second.
        for i in 0..200u32 {
            for ev in flush_group(50_000 + i * 9) {
                store
                    .insert_event_captured_at("S1", &ev, 1_000_000 + i64::from(i / 5))
                    .unwrap();
            }
        }
        // A stale tail on the live run: its last flush's records were only fetched
        // six hours later. Still exhaust, never an anchor.
        for ev in flush_group(50_000 + 200 * 9) {
            store.insert_event_captured_at("S1", &ev, 1_000_040 + 6 * 3600).unwrap();
        }
        // Stale exhaust: a failed evening sync's 100 groups, fetched next morning
        // in one second. Structurally exhaust (deleted), but not live (no anchors).
        for i in 0..100u32 {
            for ev in flush_group(90_000 + i * 9) {
                store.insert_event_captured_at("S1", &ev, 1_040_000).unwrap();
            }
        }
        // A run that looks live (30 groups fetched over 3 s) but was actually
        // generated 40 minutes earlier: the ring's own time_sync says the counter
        // had already passed 200_000 at 1_040_000, so ds 150_000 cannot be from
        // 1_042_400. Deleted like any exhaust; never an anchor.
        let ring_anchor = RingEvent {
            tag: 0x42,
            name: "time_sync".into(),
            timestamp: 200_000,
            body: vec![],
            decoded: Some(serde_json::json!({ "unix_time": 1_040_000 })),
        };
        store.insert_event_captured_at("S1", &ring_anchor, 1_040_000).unwrap();
        for i in 0..30u32 {
            for ev in flush_group(150_000 + i * 9) {
                store
                    .insert_event_captured_at("S1", &ev, 1_042_400 + i64::from(i / 10))
                    .unwrap();
            }
        }
        let (anchors, deleted) = store.compact_exhaust("S1").unwrap();

        let stale_near_ring: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE tag = 0x42 AND ring_timestamp BETWEEN 150000 AND 151000",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stale_near_ring, 0, "out of order with the ring's own anchor: rejected");
        assert_eq!(deleted, 331 * 4, "both exhaust runs' debug_events and the stale tail go; organic groups and every debug_data stay");
        assert_eq!(anchors, 2, "40 s of live capture at one anchor per 30 s; none from the stale tail");
        let late: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE tag = 0x42 AND captured_unix > 1000100
                    AND json_extract(decoded_json, '$.source') = 'phone'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(late, 0, "the stale tail row is not an anchor");
        let organic_events: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events WHERE tag = 0x43", [], |r| r.get(0))
            .unwrap();
        assert_eq!(organic_events, 30 * 4, "organic check_sleep groups untouched");
        let debug_data: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events WHERE tag = 0x61", [], |r| r.get(0))
            .unwrap();
        assert_eq!(debug_data, (30 + 331) * 2, "debug_data is never deleted");
        let (anchor_ts, anchor_captured): (i64, i64) = store
            .conn
            .query_row(
                "SELECT ring_timestamp, captured_unix FROM events WHERE tag = 0x42 ORDER BY 1 LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(anchor_ts, 50_000, "anchor at the first live exhaust record");
        assert_eq!(anchor_captured, 1_000_000, "dated to its original capture");
        let stale_anchors: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE tag = 0x42 AND ring_timestamp >= 90000
                    AND json_extract(decoded_json, '$.source') = 'phone'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stale_anchors, 0, "a bulk-fetched stale block yields no anchor");
        assert_eq!(store.compact_exhaust("S1").unwrap(), (0, 0), "idempotent");
    }

    #[test]
    fn full_database_keeps_the_previous_checkpoint() {
        let store = Store::open_in_memory().unwrap();
        store.set_cursor("S1", 7).unwrap();
        let pages: u32 = store
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        store
            .conn
            .execute_batch(&format!("PRAGMA max_page_count={pages};"))
            .unwrap();
        let mut event = sample_event();
        event.body = vec![42; 1024 * 1024];
        let error = store.commit_batch("S1", &[event], 43).unwrap_err();
        assert!(matches!(
            error,
            crate::error::Error::Sqlite { code: 13, .. }
        ));
        assert_eq!(store.cursor("S1").unwrap(), 7);
        assert!(store.event_counts("S1").unwrap().is_empty());
    }

    #[test]
    fn failed_cursor_commit_rolls_back_entire_batch() {
        let store = Store::open_in_memory().unwrap();
        store.set_cursor("S1", 7).unwrap();
        store.conn.execute_batch("CREATE TRIGGER fail_cursor BEFORE UPDATE ON sync_state BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END;").unwrap();
        let error = store.commit_batch("S1", &[sample_event()], 43).unwrap_err();
        assert!(matches!(
            error,
            crate::error::Error::Sqlite { code: 19, .. }
        ));
        assert_eq!(store.cursor("S1").unwrap(), 7);
        assert!(store.event_counts("S1").unwrap().is_empty());
        store
            .conn
            .execute_batch("DROP TRIGGER fail_cursor;")
            .unwrap();
        assert_eq!(store.commit_batch("S1", &[sample_event()], 43).unwrap(), 1);
        assert_eq!(store.commit_batch("S1", &[sample_event()], 43).unwrap(), 0);
        assert_eq!(store.cursor("S1").unwrap(), 43);
    }

    #[test]
    fn failed_insert_rolls_back_earlier_rows_and_cursor() {
        let store = Store::open_in_memory().unwrap();
        store.conn.execute_batch("CREATE TRIGGER fail_row BEFORE INSERT ON events WHEN NEW.ring_timestamp=99 BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END;").unwrap();
        let mut bad = sample_event();
        bad.timestamp = 99;
        assert!(store
            .commit_batch("S1", &[sample_event(), bad], 100)
            .is_err());
        assert!(store.event_counts("S1").unwrap().is_empty());
        assert_eq!(store.cursor("S1").unwrap(), 0);
    }

    #[test]
    fn read_only_open_does_not_initialize_schema_or_create_file() {
        let path = std::env::temp_dir().join(format!("oura-missing-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert!(Store::open_read_only(&path).is_err());
        assert!(!path.exists());
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE sentinel(value);").unwrap();
        }
        let reader = Store::open_read_only(&path).unwrap();
        assert!(reader.event_counts("S1").is_err());
        assert_eq!(reader.integrity_check().unwrap(), "ok");
        drop(reader);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn open_enables_wal_on_writable_file() {
        let dir = std::env::temp_dir().join(format!("oura-store-wal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wal.db");
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path).unwrap();
        let mode: String = store
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reader_survives_open_writer_transaction() {
        let dir = std::env::temp_dir().join(format!("oura-store-rw-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("shared.db");
        let _ = std::fs::remove_file(&path);
        let writer = Store::open(&path).unwrap();
        writer.insert_event("S1", &sample_event()).unwrap();
        // Hold an uncommitted write open — under WAL a reader still gets a
        // consistent snapshot instead of SQLITE_BUSY / a partial read.
        writer.conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        writer
            .conn
            .execute(
                "INSERT INTO readings (serial, kind, value, unit, captured_unix)
                 VALUES ('S1', 'battery_percent', 50.0, '%', 0)",
                [],
            )
            .unwrap();
        let reader = Store::open_read_only(&path).unwrap();
        let counts = reader.event_counts("S1").unwrap();
        assert_eq!(counts, vec![("debug_event".to_string(), 1)]);
        writer.conn.execute_batch("COMMIT;").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn events_dedup_and_cursor_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let ev = RingEvent {
            tag: 0x43,
            name: "debug_event",
            timestamp: 42,
            body: vec![1, 2, 3],
            decoded: None,
        };
        assert!(store.insert_event("S1", &ev).unwrap());
        assert!(!store.insert_event("S1", &ev).unwrap()); // duplicate ignored

        store.set_cursor("S1", 1234).unwrap();
        assert_eq!(store.cursor("S1").unwrap(), 1234);

        let counts = store.event_counts("S1").unwrap();
        assert_eq!(counts, vec![("debug_event".to_string(), 1)]);
    }

    #[test]
    fn decoded_events_preserve_capture_order_across_clock_reset() {
        let store = Store::open_in_memory().unwrap();
        for timestamp in [5_000_000, 10] {
            let event = RingEvent {
                tag: 0x42,
                name: "time_sync",
                timestamp,
                body: vec![0, 0, 0, 0],
                decoded: Some(serde_json::json!({"unix_time": 1_700_000_000})),
            };
            assert!(store.insert_event("S1", &event).unwrap());
        }
        let timestamps: Vec<i64> = store
            .decoded_events()
            .unwrap()
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(timestamps, [5_000_000, 10]);
    }
}
