use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};

use prasasti_query::PrasastiEngine;
use prasasti_storage::{SegmentZoneMap, StorageTierHook};
use tempfile::tempdir;

struct TierRecorder {
    zones: Mutex<Vec<SegmentZoneMap>>,
}

impl StorageTierHook for TierRecorder {
    fn on_segment_flushed(&self, zone_map: &SegmentZoneMap) {
        self.zones.lock().unwrap().push(zone_map.clone());
    }
}

#[test]
fn integration_crash_recovery_zone_pruning_and_timesfm_pipeline() {
    let dir = tempdir().unwrap();
    let recorder = Arc::new(TierRecorder {
        zones: Mutex::new(Vec::new()),
    });

    {
        let mut engine = PrasastiEngine::open(dir.path(), 4)
            .unwrap()
            .with_storage_tier_hook(recorder.clone());

        // Flush Segment 1 (4 rows: 1000..=4000)
        for i in 1..=4 {
            engine
                .append("XAUUSD", i * 1_000, 2300.0 + (i as f64), 1.0, 0)
                .unwrap();
        }
        // Flush Segment 2 (4 rows: 10000..=13000)
        for i in 10..=13 {
            engine
                .append("XAUUSD", i * 1_000, 2300.0 + (i as f64), 2.0, 0)
                .unwrap();
        }
        // Add 2 uncommitted rows in active WAL (20000, 21000)
        engine.append("XAUUSD", 20_000, 2400.0, 5.0, 0).unwrap();
        engine.append("XAUUSD", 21_000, 2410.0, 5.0, 0).unwrap();
        engine.sync().unwrap();
    }

    assert_eq!(recorder.zones.lock().unwrap().len(), 2);

    // Simulate power failure mid-write by appending 15 garbage bytes to active.wal
    {
        let wal_path = dir.path().join("active.wal");
        let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(&[0xFF; 15]).unwrap();
        f.sync_all().unwrap();
    }

    // Reopen engine and verify 100% recovery of 2 segments + 2 active WAL records
    let reopened = PrasastiEngine::open(dir.path(), 4).unwrap();
    assert_eq!(reopened.recovered_truncated_bytes(), 15);
    assert_eq!(reopened.segment_count(), 2);
    assert_eq!(reopened.memtable_len(), 2);

    // Query [10_000, 13_000] -> Segment 1 must be pruned!
    let sum = reopened
        .aggregate_range(Some("XAUUSD"), 10_000, 13_000)
        .unwrap();
    assert_eq!(sum.count, 4);
    assert_eq!(sum.scanned_segments, 1);
    assert_eq!(sum.skipped_segments, 1);

    // Full range query [0, 30_000] -> 10 total records
    let full = reopened
        .aggregate_range(Some("XAUUSD"), 0, 30_000)
        .unwrap();
    assert_eq!(full.count, 10);

    // TimesFM patch extraction (patch_len = 5 -> 2 patches of 5 floats)
    let patches = reopened
        .extract_timesfm_patches("XAUUSD", 0, 30_000, 5)
        .unwrap();
    assert_eq!(patches.len(), 2);
    assert_eq!(patches[0].len(), 5);
}
