use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::Stream;

use crate::batch::{
    Batch, BatchId, BatchInput, BatchStatus, File, FileContentItem, FileFilter, FileId,
    FileStreamItem, FileStreamResult, ListBatchesFilter, OutputFileType, RequestTemplateInput,
};
use crate::daemon::DaemonConfig;
use crate::http::{HttpClient, ReqwestHttpClient};
use crate::manager::{DaemonStorage, ModelFilter, ModelFilterState, Storage};
use crate::processor::RequestProcessor;
use crate::request::{
    AnyRequest, CascadeTargetState, Claimed, CreateFlexInput, CreateRealtimeInput, DaemonId,
    ListRequestsFilter, PersistCompletedRealtimeInput, Request, RequestDetail, RequestId,
    RequestListResult, RequestState, ServiceTierFilter,
};

type ArsenalPostgresManager<P> = fusillade_arsenal::PostgresRequestManager<P>;

pub struct PostgresRequestManager<
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient = ReqwestHttpClient,
> {
    storage: Arc<ArsenalPostgresManager<P>>,
    http_client: Arc<H>,
    config: DaemonConfig,
    processor: std::sync::OnceLock<Arc<dyn RequestProcessor<ArsenalPostgresManager<P>, H>>>,
}

impl From<&DaemonConfig> for fusillade_arsenal::PostgresStorageConfig {
    fn from(config: &DaemonConfig) -> Self {
        Self {
            pending_request_counts_timeout_ms: config.pending_request_counts_timeout_ms,
            batch_metadata_fields: config.batch_metadata_fields.clone(),
            claim_timeout_ms: config.claim_timeout_ms,
            processing_timeout_ms: config.processing_timeout_ms,
            stale_daemon_threshold_ms: config.stale_daemon_threshold_ms,
            unclaim_batch_size: config.unclaim_batch_size,
            service_tier_completion_windows_ms: config.service_tier_completion_windows_ms.clone(),
            default_completion_window_ms: config.default_completion_window_ms,
            claim_ramp_exponent: config.claim_ramp_exponent,
            urgency_weight: config.urgency_weight,
            batch_claim_require_live: config.batch_claim_require_live,
            leaks_per_window: config.leaks_per_window,
            model_filters_keep_per_model: config.model_filters_keep_per_model,
            model_filters_retention_ms: config.model_filters_retention_ms,
            ..Default::default()
        }
    }
}

impl<P: fusillade_arsenal::PoolProvider> PostgresRequestManager<P, ReqwestHttpClient> {
    pub fn new(pools: P, config: DaemonConfig) -> Self {
        let http_client = Arc::new(ReqwestHttpClient::new(
            std::time::Duration::from_millis(config.first_chunk_timeout_ms),
            std::time::Duration::from_millis(config.chunk_timeout_ms),
            std::time::Duration::from_millis(config.body_timeout_ms),
            config.streamable_endpoints.clone(),
        ));
        Self::from_parts(pools, http_client, config)
    }
}

impl<P, H> PostgresRequestManager<P, H>
where
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient + 'static,
{
    pub fn with_client(pools: P, http_client: Arc<H>) -> Self {
        Self::from_parts(pools, http_client, DaemonConfig::default())
    }

    fn from_parts(pools: P, http_client: Arc<H>, config: DaemonConfig) -> Self {
        let storage = fusillade_arsenal::PostgresRequestManager::new(pools, (&config).into());
        Self {
            storage: Arc::new(storage),
            http_client,
            config,
            processor: std::sync::OnceLock::new(),
        }
    }

    pub fn with_config(mut self, config: DaemonConfig) -> Self {
        let storage_config = (&config).into();
        Arc::get_mut(&mut self.storage)
            .expect("with_config must be called before sharing the manager")
            .set_config(storage_config);
        self.config = config;
        self
    }

    pub fn with_db_retry_config(mut self, config: fusillade_arsenal::DbRetryConfig) -> Self {
        Arc::get_mut(&mut self.storage)
            .expect("with_db_retry_config must be called before sharing the manager")
            .set_db_retry_config(config);
        self
    }

    pub fn with_download_buffer_size(mut self, buffer_size: usize) -> Self {
        Arc::get_mut(&mut self.storage)
            .expect("with_download_buffer_size must be called before sharing the manager")
            .set_download_buffer_size(buffer_size);
        self
    }

    pub fn with_batch_insert_strategy(
        mut self,
        strategy: fusillade_arsenal::BatchInsertStrategy,
    ) -> Self {
        Arc::get_mut(&mut self.storage)
            .expect("with_batch_insert_strategy must be called before sharing the manager")
            .set_batch_insert_strategy(strategy);
        self
    }

    pub fn with_processor(
        self,
        processor: Arc<dyn RequestProcessor<ArsenalPostgresManager<P>, H>>,
    ) -> Self {
        let _ = self.processor.set(processor);
        self
    }

    pub fn set_processor(
        &self,
        processor: Arc<dyn RequestProcessor<ArsenalPostgresManager<P>, H>>,
    ) -> std::result::Result<(), &'static str> {
        self.processor
            .set(processor)
            .map_err(|_| "processor already set")
    }

    pub fn set_response_transformer(
        &self,
        transformer: Arc<dyn crate::transform::ResponseTransformer>,
    ) -> std::result::Result<(), &'static str> {
        self.storage
            .set_response_transformer(Arc::new(ResponseTransformerAdapter(transformer)))
    }

    pub fn http_client(&self) -> &Arc<H> {
        &self.http_client
    }

    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }

    pub fn processor(&self) -> Option<&Arc<dyn RequestProcessor<ArsenalPostgresManager<P>, H>>> {
        self.processor.get()
    }

    pub fn storage(&self) -> &Arc<ArsenalPostgresManager<P>> {
        &self.storage
    }
}

impl<P, H> Deref for PostgresRequestManager<P, H>
where
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient,
{
    type Target = ArsenalPostgresManager<P>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

#[async_trait]
impl<P, H> Storage for PostgresRequestManager<P, H>
where
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient + 'static,
{
    async fn create_file(
        &self,
        name: String,
        description: Option<String>,
        templates: Vec<RequestTemplateInput>,
    ) -> crate::Result<FileId> {
        self.storage.create_file(name, description, templates).await
    }

    async fn create_file_stream<S: Stream<Item = FileStreamItem> + Send + Unpin>(
        &self,
        stream: S,
    ) -> crate::Result<FileStreamResult> {
        self.storage.create_file_stream(stream).await
    }

    async fn get_file(&self, file_id: FileId) -> crate::Result<File> {
        self.storage.get_file(file_id).await
    }

    async fn get_file_from_primary_pool(&self, file_id: FileId) -> crate::Result<File> {
        self.storage.get_file_from_primary_pool(file_id).await
    }

    async fn list_files(&self, filter: FileFilter) -> crate::Result<Vec<File>> {
        self.storage.list_files(filter).await
    }

    async fn get_file_content(&self, file_id: FileId) -> crate::Result<Vec<FileContentItem>> {
        self.storage.get_file_content(file_id).await
    }

    fn get_file_content_stream(
        &self,
        file_id: FileId,
        offset: usize,
        search: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = crate::Result<FileContentItem>> + Send>> {
        self.storage
            .get_file_content_stream(file_id, offset, search)
    }

    async fn get_file_template_stats(
        &self,
        file_id: FileId,
    ) -> crate::Result<Vec<crate::batch::ModelTemplateStats>> {
        self.storage.get_file_template_stats(file_id).await
    }

    async fn delete_file(&self, file_id: FileId) -> crate::Result<()> {
        self.storage.delete_file(file_id).await
    }

    async fn create_batch(&self, input: BatchInput) -> crate::Result<Batch> {
        self.storage.create_batch(input).await
    }

    async fn create_batch_record(&self, input: BatchInput) -> crate::Result<Batch> {
        self.storage.create_batch_record(input).await
    }

    async fn populate_batch(&self, batch_id: BatchId, file_id: FileId) -> crate::Result<()> {
        self.storage.populate_batch(batch_id, file_id).await
    }

    async fn get_batch(&self, batch_id: BatchId) -> crate::Result<Batch> {
        self.storage.get_batch(batch_id).await
    }

    async fn get_batch_status(&self, batch_id: BatchId) -> crate::Result<BatchStatus> {
        self.storage.get_batch_status(batch_id).await
    }

    async fn list_file_batches(&self, file_id: FileId) -> crate::Result<Vec<BatchStatus>> {
        self.storage.list_file_batches(file_id).await
    }

    async fn list_batches(&self, filter: ListBatchesFilter) -> crate::Result<Vec<Batch>> {
        self.storage.list_batches(filter).await
    }

    async fn get_batch_by_output_file_id(
        &self,
        file_id: FileId,
        file_type: OutputFileType,
    ) -> crate::Result<Option<Batch>> {
        self.storage
            .get_batch_by_output_file_id(file_id, file_type)
            .await
    }

    async fn get_batch_requests(&self, batch_id: BatchId) -> crate::Result<Vec<AnyRequest>> {
        self.storage.get_batch_requests(batch_id).await
    }

    fn get_batch_results_stream(
        &self,
        batch_id: BatchId,
        offset: usize,
        search: Option<String>,
        status: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = crate::Result<crate::batch::BatchResultItem>> + Send>> {
        self.storage
            .get_batch_results_stream(batch_id, offset, search, status)
    }

    async fn get_cancelled_batch_ids(&self, batch_ids: &[BatchId]) -> crate::Result<Vec<BatchId>> {
        self.storage.get_cancelled_batch_ids(batch_ids).await
    }

    async fn cancel_batch(&self, batch_id: BatchId) -> crate::Result<()> {
        self.storage.cancel_batch(batch_id).await
    }

    async fn cascade_batch_state_to_requests(
        &self,
        batch_id: BatchId,
        target_state: CascadeTargetState,
    ) -> crate::Result<u64> {
        self.storage
            .cascade_batch_state_to_requests(batch_id, target_state)
            .await
    }

    async fn delete_batch(&self, batch_id: BatchId) -> crate::Result<()> {
        self.storage.delete_batch(batch_id).await
    }

    async fn delete_request(&self, request_id: RequestId) -> crate::Result<()> {
        self.storage.delete_request(request_id).await
    }

    async fn bulk_delete_data(&self, creator_id: &str, batch_size: i64) -> crate::Result<u64> {
        self.storage.bulk_delete_data(creator_id, batch_size).await
    }

    async fn retry_failed_requests(
        &self,
        ids: Vec<RequestId>,
    ) -> crate::Result<Vec<crate::Result<()>>> {
        self.storage.retry_failed_requests(ids).await
    }

    async fn retry_failed_requests_for_batch(&self, batch_id: BatchId) -> crate::Result<u64> {
        self.storage.retry_failed_requests_for_batch(batch_id).await
    }

    async fn get_pending_request_counts_by_model_and_window(
        &self,
        windows: &[(String, Option<i64>, i64)],
        states: &[String],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
        priority_decay_window: Option<i64>,
        strict: bool,
    ) -> crate::Result<std::collections::HashMap<String, std::collections::HashMap<String, i64>>>
    {
        self.storage
            .get_pending_request_counts_by_model_and_window(
                windows,
                states,
                model_filter,
                service_tier_filter,
                priority_decay_window,
                strict,
            )
            .await
    }

    async fn sum_owner_batch_requests_in_window(
        &self,
        owner: &str,
        completion_window: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
    ) -> crate::Result<i64> {
        self.storage
            .sum_owner_batch_requests_in_window(owner, completion_window, cutoff, strict)
            .await
    }

    async fn count_owner_flex_requests_since(
        &self,
        owner: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
    ) -> crate::Result<i64> {
        self.storage
            .count_owner_flex_requests_since(owner, cutoff, strict)
            .await
    }

    async fn get_requests(
        &self,
        ids: Vec<RequestId>,
    ) -> crate::Result<Vec<crate::Result<AnyRequest>>> {
        self.storage.get_requests(ids).await
    }

    async fn claim_batchless_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &std::collections::HashSet<(String, String, String)>,
    ) -> crate::Result<Vec<Request<Claimed>>> {
        self.storage
            .claim_batchless_requests(
                limit,
                daemon_id,
                available_capacity,
                user_active_counts,
                leak_cooldown,
            )
            .await
    }

    async fn claim_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &std::collections::HashSet<(String, String, String)>,
    ) -> crate::Result<Vec<Request<Claimed>>> {
        self.storage
            .claim_requests(
                limit,
                daemon_id,
                available_capacity,
                user_active_counts,
                leak_cooldown,
            )
            .await
    }

    async fn claim_batch_requests(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> crate::Result<Vec<Request<Claimed>>> {
        self.storage
            .claim_batch_requests(
                limit,
                batch_limit,
                daemon_id,
                available_capacity,
                user_active_counts,
            )
            .await
    }

    fn supports_batch_claims(&self) -> bool {
        self.storage.supports_batch_claims()
    }

    async fn append_model_filter_event(&self, entry: &ModelFilter) -> crate::Result<()> {
        self.storage.append_model_filter_event(entry).await
    }

    async fn append_model_filter_events(&self, entries: &[ModelFilter]) -> crate::Result<()> {
        self.storage.append_model_filter_events(entries).await
    }

    async fn list_model_filters(&self) -> crate::Result<Vec<ModelFilter>> {
        self.storage.list_model_filters().await
    }

    async fn current_filter_states(
        &self,
    ) -> crate::Result<
        std::collections::HashMap<String, (ModelFilterState, chrono::DateTime<chrono::Utc>)>,
    > {
        self.storage.current_filter_states().await
    }

    async fn persist<T: RequestState + Clone>(
        &self,
        request: &Request<T>,
    ) -> crate::Result<Option<RequestId>>
    where
        AnyRequest: From<Request<T>>,
    {
        self.storage.persist(request).await
    }

    async fn reschedule_for_retry(
        &self,
        request_id: RequestId,
        owner: DaemonId,
        retry_attempt: u32,
        not_before: Option<chrono::DateTime<chrono::Utc>>,
    ) -> crate::Result<bool> {
        self.storage
            .reschedule_for_retry(request_id, owner, retry_attempt, not_before)
            .await
    }

    async fn list_requests(&self, filter: ListRequestsFilter) -> crate::Result<RequestListResult> {
        self.storage.list_requests(filter).await
    }

    async fn get_request_detail(&self, request_id: RequestId) -> crate::Result<RequestDetail> {
        self.storage.get_request_detail(request_id).await
    }

    async fn create_realtime(&self, input: CreateRealtimeInput) -> crate::Result<RequestId> {
        self.storage.create_realtime(input).await
    }

    async fn create_flex(&self, input: CreateFlexInput) -> crate::Result<RequestId> {
        self.storage.create_flex(input).await
    }

    async fn complete_request(
        &self,
        request_id: RequestId,
        response_body: &str,
        status_code: u16,
    ) -> crate::Result<()> {
        self.storage
            .complete_request(request_id, response_body, status_code)
            .await
    }

    async fn fail_request(
        &self,
        request_id: RequestId,
        error: &str,
        status_code: u16,
    ) -> crate::Result<()> {
        self.storage
            .fail_request(request_id, error, status_code)
            .await
    }

    async fn persist_completed_realtime_batch(
        &self,
        records: &[PersistCompletedRealtimeInput],
    ) -> crate::Result<()> {
        self.storage.persist_completed_realtime_batch(records).await
    }
}

#[async_trait]
impl<P, H> DaemonStorage for PostgresRequestManager<P, H>
where
    P: fusillade_arsenal::PoolProvider,
    H: HttpClient + 'static,
{
    async fn persist_daemon<T: crate::daemon::DaemonState + Clone>(
        &self,
        record: &crate::daemon::DaemonRecord<T>,
    ) -> crate::Result<()>
    where
        crate::daemon::AnyDaemonRecord: From<crate::daemon::DaemonRecord<T>>,
    {
        self.storage.persist_daemon(record).await
    }

    async fn get_daemon(
        &self,
        daemon_id: DaemonId,
    ) -> crate::Result<crate::daemon::AnyDaemonRecord> {
        self.storage.get_daemon(daemon_id).await
    }

    async fn list_daemons(
        &self,
        status_filter: Option<crate::daemon::DaemonStatus>,
    ) -> crate::Result<Vec<crate::daemon::AnyDaemonRecord>> {
        self.storage.list_daemons(status_filter).await
    }

    async fn purge_orphaned_rows(&self, batch_size: i64) -> crate::Result<u64> {
        self.storage.purge_orphaned_rows(batch_size).await
    }

    async fn purge_model_filter_events(
        &self,
        batch_size: i64,
        keep_per_model: i64,
        retention_secs: f64,
    ) -> crate::Result<u64> {
        self.storage
            .purge_model_filter_events(batch_size, keep_per_model, retention_secs)
            .await
    }
}

struct ResponseTransformerAdapter(Arc<dyn crate::transform::ResponseTransformer>);

#[async_trait]
impl fusillade_arsenal::ResponseTransformer for ResponseTransformerAdapter {
    async fn transform(
        &self,
        request: &crate::request::RequestData,
        body: &str,
    ) -> crate::Result<String> {
        self.0.transform(request, body).await
    }
}
