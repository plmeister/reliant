use reliant_indicators::{PivotKind, SwingPivot};
use reliant_strategy::{evaluate, EntrySignal};

#[test]
fn evaluate_rejects_zero_atr() {
    assert!(evaluate(&SwingPivot { index: 3, kind: PivotKind::Low, price: 90.0, atr_confirm: 0.0 }).is_none());
}

#[test]
fn evaluate_accepts_valid_pivot() {
    let sig = SwingPivot { index: 3, kind: PivotKind::Low, price: 90.0, atr_confirm: 2.5 };
    assert_eq!(
        evaluate(&sig),
        Some(EntrySignal { index: 3, kind: PivotKind::Low, price: 90.0 })
    );
}
