use reliant_portfolio::Registry;
use std::collections::BTreeSet;
use std::path::PathBuf;

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("atlas-pf-test-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_pf(dir: &std::path::Path, id: &str, instruments: &[&str], timeframes: &[&str]) {
    let d = dir.join(id);
    std::fs::create_dir_all(&d).unwrap();
    let tf_json = timeframes
        .iter()
        .map(|t| format!("\"{}\"", t))
        .collect::<Vec<_>>()
        .join(",");
    let json = format!(
        "{{\"version\":1,\"id\":\"{}\",\"instruments\":[{}],\"timeframes\":[{}],\"tradeTimeframe\":\"{}\",\"contextTimeframes\":[],\"strategy\":{{\"file\":null,\"params\":{{\"k\":3.5}}}}}}",
        id,
        instruments.iter().map(|i| format!("\"{}\"", i)).collect::<Vec<_>>().join(","),
        tf_json,
        timeframes[0]
    );
    std::fs::write(d.join("portfolio.json"), json).unwrap();
}

#[test]
fn loads_single_valid_portfolio() {
    let root = tmp_dir("single");
    write_pf(&root, "audcad_mr", &["AUDCAD"], &["1d"]);
    let reg = Registry::load(&root).unwrap();
    assert_eq!(reg.len(), 1);
    let pf = reg.get("audcad_mr").unwrap();
    assert_eq!(pf.instruments, vec!["AUDCAD"]);
    assert_eq!(pf.trade_timeframe.as_str(), "1d");
    assert_eq!(pf.all_timeframes().len(), 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn rejects_id_mismatch_with_directory() {
    let root = tmp_dir("mismatch");
    let dir = root.join("wrongname");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("portfolio.json"),
        r#"{"version":1,"id":"other","instruments":["AUDCAD"],"timeframes":["1d"],"tradeTimeframe":"1d"}"#,
    )
    .unwrap();
    let reg = Registry::load(&root).unwrap();
    assert_eq!(reg.len(), 0);
    assert_eq!(reg.failures().len(), 1);
    assert!(reg.failures()[0].contains("other"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn rejects_unknown_timeframe() {
    let root = tmp_dir("badtf");
    let dir = root.join("badpf");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("portfolio.json"),
        r#"{"version":1,"id":"badpf","instruments":["AUDCAD"],"timeframes":["1x"],"tradeTimeframe":"1x"}"#,
    )
    .unwrap();
    let reg = Registry::load(&root).unwrap();
    assert_eq!(reg.len(), 0);
    assert_eq!(reg.failures().len(), 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn rejects_trade_timeframe_not_in_list() {
    let root = tmp_dir("badtrade");
    let dir = root.join("badtrade_pf");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("portfolio.json"),
        r#"{"version":1,"id":"badtrade_pf","instruments":["AUDCAD"],"timeframes":["1w"],"tradeTimeframe":"1d"}"#,
    )
    .unwrap();
    let reg = Registry::load(&root).unwrap();
    assert_eq!(reg.len(), 0);
    assert_eq!(reg.failures().len(), 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn collects_instrument_universe_across_portfolios() {
    let root = tmp_dir("universe");
    write_pf(&root, "audcad_mr", &["AUDCAD", "EURUSD"], &["1d"]);
    write_pf(&root, "eurusd_swing", &["EURUSD", "AUDNZD"], &["1w"]);
    let reg = Registry::load(&root).unwrap();
    let uni: BTreeSet<String> = reg.instrument_universe().into_iter().collect();
    assert_eq!(uni.len(), 3);
    assert_eq!(reg.instrument_timeframes("EURUSD").len(), 2);
    let _ = std::fs::remove_dir_all(&root);
}
