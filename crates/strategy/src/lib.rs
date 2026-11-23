use reliant_indicators::{PivotKind, SwingPivot};

#[derive(Debug, Clone, PartialEq)]
pub struct EntrySignal {
    pub index: i64,
    pub kind: PivotKind,
    pub price: f64,
}

pub fn evaluate(sig: &SwingPivot) -> Option<EntrySignal> {
    if sig.atr_confirm <= 0.0 {
        return None;
    }
    Some(EntrySignal { index: sig.index, kind: sig.kind, price: sig.price })
}
