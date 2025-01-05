mod data;
mod simulation;

use std::str::FromStr;

use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reference_date = "2022-01-01T00:00:00Z";
    let product_id = Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1")?;

    let sim_coordinator = simulation::coordinator::Orchestrator::new().await?;
    sim_coordinator
        .run_by_product(product_id, reference_date)
        .await?;

    Ok(())
}
