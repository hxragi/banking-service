use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::{Request, Status, metadata::MetadataValue};

const INTERNAL_API_KEY_HEADER: &str = "X-Internal-Api-Key";
const INTERNAL_API_KEY_BIN: &str = "x-internal-api-key-bin";

#[derive(Clone, Debug)]
pub struct InternalAuthInterceptor {
    api_key: Arc<String>,
}

impl InternalAuthInterceptor {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: Arc::new(api_key),
        }
    }
}

impl Interceptor for InternalAuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let is_internal = req
            .metadata()
            .get(INTERNAL_API_KEY_HEADER)
            .or_else(|| req.metadata().get(INTERNAL_API_KEY_BIN))
            .and_then(|v| v.to_str().ok())
            .map(|v| v == *self.api_key)
            .unwrap_or(false);

        if is_internal {
            req.metadata_mut()
                .insert("x-is-internal-request", MetadataValue::from_static("true"));
        }

        Ok(req)
    }
}

pub trait InternalRequestExt {
    fn is_internal(&self) -> bool;
}

impl<T> InternalRequestExt for Request<T> {
    fn is_internal(&self) -> bool {
        self.metadata()
            .get("x-is-internal-request")
            .and_then(|v| v.to_str().ok())
            == Some("true")
    }
}
