pub trait Indicator {
    type Output;
    fn update(&mut self, value: f64) -> Option<Self::Output>;
    fn value(&self) -> Option<Self::Output>;
}

pub mod atr;
pub mod ema;
pub mod swing;

pub use atr::Atr;
pub use ema::Ema;
pub use swing::{Pivot, PivotKind, SwingDetector, SwingPivot};
