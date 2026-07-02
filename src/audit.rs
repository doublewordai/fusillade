use sqlx::{PgPool, postgres::PgConnectOptions};

pub const POSTGRES_APPLICATION_NAME: &str = "fusillade";

pub fn with_application_name(options: PgConnectOptions) -> PgConnectOptions {
    options.application_name(POSTGRES_APPLICATION_NAME)
}

pub async fn log_postgres_audit_status(pool: &PgPool) {
    let database_name: String = match sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
    {
        Ok(name) => name,
        Err(error) => {
            tracing::warn!(error = %error, "Failed to inspect PostgreSQL audit settings");
            return;
        }
    };

    let extension_installed: bool = match sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgaudit')",
    )
    .fetch_one(pool)
    .await
    {
        Ok(installed) => installed,
        Err(error) => {
            tracing::warn!(
                database = %database_name,
                error = %error,
                "Failed to check whether pgAudit is installed"
            );
            return;
        }
    };

    let shared_preload_libraries: Option<String> =
        match sqlx::query_scalar("SELECT current_setting('shared_preload_libraries', true)")
            .fetch_one(pool)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    database = %database_name,
                    error = %error,
                    "Failed to read shared_preload_libraries"
                );
                return;
            }
        };
    let pgaudit_log: Option<String> =
        match sqlx::query_scalar("SELECT current_setting('pgaudit.log', true)")
            .fetch_one(pool)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    database = %database_name,
                    error = %error,
                    "Failed to read pgaudit.log"
                );
                return;
            }
        };
    let log_connections: Option<String> =
        match sqlx::query_scalar("SELECT current_setting('log_connections', true)")
            .fetch_one(pool)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    database = %database_name,
                    error = %error,
                    "Failed to read log_connections"
                );
                return;
            }
        };
    let log_disconnections: Option<String> =
        match sqlx::query_scalar("SELECT current_setting('log_disconnections', true)")
            .fetch_one(pool)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    database = %database_name,
                    error = %error,
                    "Failed to read log_disconnections"
                );
                return;
            }
        };

    let preload_has_pgaudit = shared_preload_libraries
        .as_deref()
        .map(|value| value.split(',').any(|entry| entry.trim() == "pgaudit"))
        .unwrap_or(false);
    let pgaudit_log_configured = pgaudit_log
        .as_deref()
        .map(|value| {
            let value = value.trim();
            !value.is_empty() && value != "none"
        })
        .unwrap_or(false);
    let connection_logging_enabled = matches!(log_connections.as_deref(), Some("on"))
        && matches!(log_disconnections.as_deref(), Some("on"));

    if extension_installed
        && preload_has_pgaudit
        && pgaudit_log_configured
        && connection_logging_enabled
    {
        tracing::info!(
            database = %database_name,
            shared_preload_libraries = ?shared_preload_libraries,
            pgaudit_log = ?pgaudit_log,
            log_connections = ?log_connections,
            log_disconnections = ?log_disconnections,
            "PostgreSQL audit settings detected"
        );
    } else {
        tracing::warn!(
            database = %database_name,
            pg_audit_extension_installed = extension_installed,
            shared_preload_libraries = ?shared_preload_libraries,
            pgaudit_log = ?pgaudit_log,
            log_connections = ?log_connections,
            log_disconnections = ?log_disconnections,
            "PostgreSQL audit settings are incomplete"
        );
    }
}
