//! Where writes go. Both front-ends hand every write to an [`Executor`] as
//! a [`Command`], so standalone and replicated servers share one
//! implementation of every write's semantics ([`state_machine::apply`]).

use emberkv_engine::Engine;
use tracing::error;

use crate::state_machine::{self, Command, Outcome};

#[derive(Debug, Clone)]
pub(crate) enum Executor {
    /// Apply directly to the local engine.
    Standalone(Engine),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecuteError {
    #[error(transparent)]
    Storage(#[from] emberkv_engine::Error),
    #[error("the write task panicked")]
    Panicked,
}

impl Executor {
    pub(crate) async fn execute(&self, command: Command) -> Result<Outcome, ExecuteError> {
        match self {
            Executor::Standalone(engine) => {
                let engine = engine.clone();
                // Engine calls block on disk I/O (and possibly fsync).
                let applied = tokio::task::spawn_blocking(move || {
                    engine.transaction(|tx| state_machine::apply(tx, &command))
                })
                .await;
                match applied {
                    Ok(result) => result.map_err(ExecuteError::from),
                    Err(err) => {
                        error!(error = %err, "write task panicked");
                        Err(ExecuteError::Panicked)
                    }
                }
            }
        }
    }
}
