use reliant_datamodel::{Candle, Event, Symbol, Timeframe};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::{Float64Array, TimestampNanosecondArray};
use std::fs::File;

pub fn write_parquet(path: &str, candles: &[Candle]) -> Result<(), String> {
    use arrow::array::{Float64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));

    let ts: Vec<i64> = candles.iter().map(|c| c.timestamp.timestamp_nanos_opt().unwrap_or(0)).collect();
    let open: Vec<f64> = candles.iter().map(|c| c.open).collect();
    let high: Vec<f64> = candles.iter().map(|c| c.high).collect();
    let low: Vec<f64> = candles.iter().map(|c| c.low).collect();
    let close: Vec<f64> = candles.iter().map(|c| c.close).collect();
    let volume: Vec<f64> = candles.iter().map(|c| c.volume).collect();

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")),
        Arc::new(Float64Array::from(open)),
        Arc::new(Float64Array::from(high)),
        Arc::new(Float64Array::from(low)),
        Arc::new(Float64Array::from(close)),
        Arc::new(Float64Array::from(volume)),
    ]).map_err(|e| e.to_string())?;

    let file = File::create(path).map_err(|e| e.to_string())?;
    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

pub fn read_parquet(path: &str) -> Result<Vec<Candle>, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let reader = builder.build().map_err(|e| e.to_string())?;
    let mut candles = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| "timestamp column type mismatch".to_string())?;
        let open = batch.column(1).as_any().downcast_ref::<Float64Array>().ok_or("open column type mismatch")?;
        let high = batch.column(2).as_any().downcast_ref::<Float64Array>().ok_or("high column type mismatch")?;
        let low = batch.column(3).as_any().downcast_ref::<Float64Array>().ok_or("low column type mismatch")?;
        let close = batch.column(4).as_any().downcast_ref::<Float64Array>().ok_or("close column type mismatch")?;
        let volume = batch.column(5).as_any().downcast_ref::<Float64Array>().ok_or("volume column type mismatch")?;
        for i in 0..batch.num_rows() {
            candles.push(Candle {
                timestamp: chrono::DateTime::from_timestamp_nanos(ts.value(i)),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume: volume.value(i),
            });
        }
    }
    Ok(candles)
}

pub trait CandleProvider {
    fn candles(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Vec<Candle>, String>;
}

pub struct ParquetDirProvider {
    pub root: std::path::PathBuf,
}

impl CandleProvider for ParquetDirProvider {
    fn candles(&self, symbol: &Symbol, timeframe: &Timeframe) -> Result<Vec<Candle>, String> {
        let path = self.root.join(format!("{}.{}.parquet", symbol.0, timeframe.as_str()));
        read_parquet(path.to_str().ok_or_else(|| "invalid path".to_string())?)
    }
}

pub fn candles_to_events(symbol: &Symbol, timeframe: &Timeframe, candles: Vec<Candle>) -> Vec<Event> {
    candles
        .into_iter()
        .map(|c| Event::CandleClosed {
            symbol: symbol.clone(),
            timeframe: timeframe.clone(),
            candle: c,
        })
        .collect()
}

pub fn interleaved_events(sources: Vec<(Symbol, Timeframe, Vec<Candle>)>) -> Vec<Event> {
    let mut rows: Vec<(i64, Symbol, Timeframe, Candle)> = Vec::new();
    for (sym, tf, candles) in sources {
        for c in candles {
            rows.push((c.timestamp.timestamp(), sym.clone(), tf.clone(), c));
        }
    }
    rows.sort_by_key(|r| r.0);

    let mut events = Vec::with_capacity(rows.len());
    let mut i = 0;
    while i < rows.len() {
        let ts = rows[i].0;
        // emit all candleclosed for this timestamp
        while i < rows.len() && rows[i].0 == ts {
            let (_, sym, tf, c) = rows[i].clone();
            events.push(Event::CandleClosed { symbol: sym, timeframe: tf, candle: c });
            i += 1;
        }
    }
    events
}
