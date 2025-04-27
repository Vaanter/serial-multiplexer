use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tracing::Level;

static TRACING_SETUP: AtomicBool = AtomicBool::new(false);
static PERMIT: Semaphore = Semaphore::const_new(1);

pub async fn setup_tracing() {
  let _permit = PERMIT.acquire().await.unwrap();
  let Ok(false) = TRACING_SETUP.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
  else {
    return;
  };
  let subscriber = tracing_subscriber::fmt()
    .with_env_filter(format!("serial_multiplexer={}", Level::TRACE))
    .with_file(true)
    .with_line_number(true)
    .with_thread_ids(true)
    .with_target(false)
    .finish();
  let _ = tracing::subscriber::set_global_default(subscriber);
}
