use ktxd::app_state::AppState;
use ktxd::config::AppConfig;
use ktxd::driver::TurnDriver;
use ktxd::responses::router;
use ktxd::session::MemoryStore;
use ktxd::substrate::{NullSeedResolver, NullSink};
use ktxd::upstream::ReqwestChatCompletionsClient;
use std::{env, path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = env::var_os("KTXD_CONFIG")
        .map(PathBuf::from)
        .or_else(|| {
            ["config.toml", "config.local.toml"]
                .into_iter()
                .map(PathBuf::from)
                .find(|path| path.is_file())
        });
    let config = Arc::new(AppConfig::load(config_path.as_deref())?);
    let store = MemoryStore::shared();
    let upstream = Arc::new(ReqwestChatCompletionsClient::default());
    let driver = Arc::new(TurnDriver::new(
        config.clone(),
        upstream,
        store.clone(),
        Arc::new(NullSink),
        Arc::new(NullSeedResolver),
    ));
    let app = router(AppState {
        config: config.clone(),
        store,
        driver,
    })
    .layer(TraceLayer::new_for_http());
    let listener = tokio::net::TcpListener::bind(config.server.bind).await?;
    tracing::info!(bind = %config.server.bind, "ktxd listening");
    axum::serve(listener, app).await?;
    Ok(())
}
