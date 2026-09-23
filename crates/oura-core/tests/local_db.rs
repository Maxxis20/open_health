//! Ignored by default: runs the production summary against a real database.
//!
//!     OURA_DB=/path/to/oura.db OURA_TZ=10800 cargo test -p oura-core --test local_db -- --ignored --nocapture
//!
//! With OURA_COMPACT=1 it first compacts the exhaust and vacuums, in place.
#[test]
#[ignore]
fn summarise_local_database() {
    let path = std::env::var("OURA_DB").expect("OURA_DB=/path/to/oura.db");
    let tz: i64 = std::env::var("OURA_TZ").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    if std::env::var("OURA_COMPACT").is_ok() {
        let store = oura_store::storage::Store::open(&path).unwrap();
        let serial = store.device_serials().unwrap().remove(0);
        let t = std::time::Instant::now();
        let (anchors, deleted) = store.compact_exhaust(&serial).unwrap();
        println!("compact: anchors={anchors} deleted={deleted} in {:.1}s", t.elapsed().as_secs_f64());
        let t = std::time::Instant::now();
        store.vacuum().unwrap();
        println!(
            "vacuum: {:.1}s, file now {} MB",
            t.elapsed().as_secs_f64(),
            std::fs::metadata(&path).unwrap().len() / 1_000_000
        );
    }
    let t = std::time::Instant::now();
    let json = oura_core::summary_json(path.clone(), tz);
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    println!("summary in {:.1}s", t.elapsed().as_secs_f64());
    println!("top-level keys: {:?}", v.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()));
    if let Some(nights) = v["nights"].as_array() {
        for n in nights {
            println!(
                "night {}",
                { let s = serde_json::to_string(n).unwrap(); if std::env::var("OURA_FULL").is_ok() { s } else { s.chars().take(260).collect::<String>() } }
            );
        }
    }
    for key in ["clock", "diagnostics", "withheld", "undated", "notes", "warnings"] {
        if !v[key].is_null() {
            println!(
                "{key}: {}",
                serde_json::to_string(&v[key]).unwrap().chars().take(700).collect::<String>()
            );
        }
    }
}
