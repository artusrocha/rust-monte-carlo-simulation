mod data;
mod simulation;

use std::str::FromStr;

use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reference_date = "2022-01-01";
    let days_to_analyze = 30;
    let product_id = Uuid::from_str("d0bd335e-fc46-408d-90fb-209ccc521fa1")?;
    let default_time_limit = 60;
    let stock_limit = 1000;

    simulation::orchestrator::run(
        reference_date,
        days_to_analyze,
        product_id,
        default_time_limit,
        stock_limit,
    )
    .await?;

    Ok(())
}
