use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use rdkafka::Message;
use rdkafka::message::{BorrowedMessage, Headers};

pub fn extract_trace_context(msg: &BorrowedMessage<'_>) -> Option<SpanContext> {
    let headers = msg.headers()?;
    let mut traceparent_value: Option<String> = None;

    for header in headers.iter() {
        if header.key == "traceparent"
            && let Some(value) = header.value
        {
            traceparent_value = Some(String::from_utf8_lossy(value).to_string());
            break;
        }
    }

    let traceparent = traceparent_value?;
    parse_traceparent(&traceparent)
}

fn parse_traceparent(traceparent: &str) -> Option<SpanContext> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let version = u8::from_str_radix(parts[0], 16).ok()?;
    if version != 0 && version != 1 {
        return None;
    }

    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    let flags = u8::from_str_radix(parts[3], 16).ok()?;
    let trace_flags = TraceFlags::new(flags);

    Some(SpanContext::new(
        trace_id,
        span_id,
        trace_flags,
        false,
        TraceState::default(),
    ))
}

pub fn create_traceparent_for_kafka(_span: &tracing::Span) -> Option<String> {
    let otel_context = opentelemetry::Context::current();
    let span = otel_context.span();
    let span_ref = span.span_context();

    if span_ref.is_valid() {
        return Some(format!(
            "{:02x}-{:032x}-{:016x}-{:02x}",
            1,
            span_ref.trace_id(),
            span_ref.span_id(),
            span_ref.trace_flags()
        ));
    }

    None
}
