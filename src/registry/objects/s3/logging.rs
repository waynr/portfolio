#[derive(Debug)]
pub(crate) struct LoggingInterceptor;

impl aws_sdk_s3::config::Interceptor for LoggingInterceptor {
    fn name(&self) -> &'static str {
        "LoggingInterceptor"
    }

    fn read_after_serialization(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let request = context.request();
        println!("read_after_serialization: request = {request:?}");
        Ok(())
    }

    fn read_after_deserialization(
        &self,
        context: &aws_sdk_s3::config::interceptors::AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let response = context.response();
        println!("read_after_deserialization: response = {response:?}");
        Ok(())
    }
}
