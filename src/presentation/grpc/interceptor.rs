use std::sync::Arc;

use tonic::{Request, Status};

pub struct InternalAuthInterceptor {
    internal_api_key: Arc<String>,
}

impl InternalAuthInterceptor {
    pub fn new(internal_api_key: String) -> Self {
        Self {
            internal_api_key: Arc::new(internal_api_key),
        }
    }
}

impl tonic::service::Interceptor for InternalAuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let internal_api_key = self.internal_api_key.clone();
        
        if let Some(header_value) = request.metadata().get("x-internal-api-key") {
            if let Ok(header_str) = header_value.to_str() {
                if header_str == internal_api_key.as_str() {
                    request.metadata_mut().insert(
                        "x-is-internal-request",
                        tonic::metadata::MetadataValue::from_static("true"),
                    );
                }
            }
        }
        
        Ok(request)
    }
}

pub trait InternalRequestExt {
    fn is_internal(&self) -> bool;
}

impl<T> InternalRequestExt for Request<T> {
    fn is_internal(&self) -> bool {
        self.metadata().get("x-is-internal-request").is_some()
    }
}
