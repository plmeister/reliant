use reliant_indicators::SwingDetector;

#[test]
fn swing_debug_synthetic() {
    let mut prices = Vec::new();
    for i in 0..200 {
        let v = 100.0 + (i as f64 * 0.3).sin() * 5.0;
        prices.push(v);
    }
    // fixed ATR 0.5, lookback 3, k=1.0
    let mut det = SwingDetector::new("swing", 1.0, 3);
    let mut sigs = 0;
    for (i, p) in prices.iter().enumerate() {
        let s = det.push(*p + 0.5, *p - 0.5, *p, i as i64, 0.5);
        sigs += s.len();
    }
    println!("SYNTHETIC fixed-atr sigs={}", sigs);

    // real ATR via apply_with_signals on a trending+oscillating series
    let mut det2 = SwingDetector::new("swing", 1.0, 3);
    let mut sigs2 = 0;
    for (i, p) in prices.iter().enumerate() {
        let c_high = *p + 0.5;
        let c_low = *p - 0.5;
        let c_close = *p;
        // build a Candle manually? apply_with_signals takes &Candle. Use push with atr=0.5 instead.
        let s = det2.push(c_high, c_low, c_close, i as i64, 0.5);
        sigs2 += s.len();
    }
    println!("SYNTHETIC2 sigs={}", sigs2);
    let _ = (det, det2, sigs, sigs2);
}
