use chrono::Duration;
use chrono::{DateTime, Utc};
use clickhouse_rs::Pool as ClickHousePool;
use futures::stream::StreamExt;
use log::{error, info, warn};
use mongodb::bson;
use mongodb::{
    bson::doc,
    options::{ClientOptions, FindOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio;

#[derive(Debug, Serialize, Deserialize)]
struct Statement {
    #[serde(rename = "_id")]
    id: bson::oid::ObjectId,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    timestamp: DateTime<Utc>,
}

#[derive(Deserialize, Clone)]
struct TenantConfig {
    name: String,
    mongo_uri: String,
    mongo_db: String,
    mongo_collection: String,
    clickhouse_uri: String,
    clickhouse_db: String,
    clickhouse_table: String,
}

#[derive(Deserialize, Clone)]
struct AppConfig {
    tenants: Vec<TenantConfig>,
    encryption_salt: String,
    batch_size: u64,
    number_of_workers: usize,
    pg_database_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load configuration from YAML file based on environment
    let env = std::env::var("ENV").unwrap_or_else(|_| "dev".to_string());
    let config_path = match env.as_str() {
        "dev" => "config-dev.yml",
        "prod" => "config-prod.yml",
        _ => {
            log::error!("Unsupported environment: {}", env);
            return Err("Unsupported environment".into());
        }
    };

    let config_file = std::fs::File::open(config_path)?;
    let app_config: AppConfig = serde_yaml::from_reader(config_file)?;

    // Iterate over tenants and process each one
    for tenant_config in app_config.tenants {
        process_tenant(&tenant_config).await?;
    }

    Ok(())
}

async fn process_tenant(tenant_config: &TenantConfig) -> Result<(), Box<dyn Error>> {
    info!("Starting tenant: {}", &tenant_config.name);
    println!(": {}", &tenant_config.clickhouse_uri);
    // Connect to MongoDB
    let client_options = match ClientOptions::parse(&tenant_config.mongo_uri).await {
        Ok(options) => options,
        Err(e) => {
            log::error!(
                "Failed to connect to MongoDB for tenant {}: {}",
                tenant_config.name,
                e
            );
            return Ok(());
        }
    };
    let client = match Client::with_options(client_options) {
        Ok(client) => client,
        Err(e) => {
            log::error!(
                "Failed to create MongoDB client for tenant {}: {}",
                tenant_config.name,
                e
            );
            return Ok(());
        }
    };
    let db = client.database(&tenant_config.mongo_db);
    let collection: mongodb::Collection<Statement> = db.collection(&tenant_config.mongo_collection);

    // Generate start and end dates for the desired period
    let end_time: DateTime<Utc> = Utc::now();
    let start_time: DateTime<Utc> = end_time - Duration::days(31);

    println!("{}", start_time);
    println!("{}", end_time);

    let count_filter = doc! {
        "timestamp": {
            "$gte": bson::DateTime::from_millis(start_time.timestamp_millis()),
            "$lt": bson::DateTime::from_millis(end_time.timestamp_millis())
        }
    };
    let expected_count = match collection.count_documents(count_filter, None).await {
        Ok(count) => count,
        Err(e) => {
            log::error!(
                "Failed to count documents for tenant {}: {}",
                tenant_config.name,
                e
            );
            return Ok(());
        }
    };
    // Define the number of parallel tasks
    let num_tasks = 10;

    // Calculate the time interval for each task
    let time_interval = (end_time - start_time) / num_tasks;

    // Define the projection
    let find_options = FindOptions::builder()
        .projection(doc! { "_id": 1, "timestamp": 1 })
        .build();

    // Retrieve record IDs in parallel using Tokio tasks
    let mut handles = Vec::new();
    for i in 0..num_tasks {
        let collection = collection.clone();
        let offset = chrono::Duration::milliseconds(time_interval.num_milliseconds() * i as i64);
        let start_time = start_time + offset;
        let end_time = start_time + time_interval;
        let filter = doc! {
            "timestamp": {
                "$gte": bson::DateTime::from_millis(start_time.timestamp_millis()),
                "$lt": bson::DateTime::from_millis(end_time.timestamp_millis())
            }
        };
        let find_options = find_options.clone();
        let handle = tokio::spawn(async move {
            let mut cursor = collection.find(filter, find_options).await.unwrap();
            let mut record_ids = Vec::new();
            while let Some(result) = cursor.next().await {
                match result {
                    Ok(document) => {
                        // println!("ID: {:?}, Timestamp: {:?}", document.id, document.timestamp);
                        record_ids.push(document.id.to_string());
                    }
                    Err(e) => eprintln!("Error retrieving document: {}", e),
                }
            }
            record_ids
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete and collect the results
    let mut all_record_ids = Vec::new();
    for handle in handles {
        let record_ids = handle.await?;
        all_record_ids.extend(record_ids);
    }

    // Print the retrieved record IDs
    all_record_ids.sort();
    // println!("Retrieved record IDs: {:?}", all_record_ids);
    let actual_count = all_record_ids.len() as i64;
    println!("Expected: {}, Actual: {}", expected_count, actual_count);

    // ClickHouse
    let clickhouse_url_with_db = format!(
        "{}/{}",
        tenant_config.clickhouse_uri, tenant_config.clickhouse_db
    );
    let clickhouse_pool = ClickHousePool::new(clickhouse_url_with_db.as_str());

    let mut clickhouse_client = clickhouse_pool.get_handle().await.unwrap();
    let mut clickhouse_total: u64 = 0;

    let clickhouse_query = format!(
        "SELECT count(DISTINCT id) FROM {}.{}_mv WHERE toDateTime64('{}', 3) < timestamp AND timestamp < toDateTime64('{}', 3)",
        tenant_config.clickhouse_db,
        tenant_config.clickhouse_table,
        start_time.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
        end_time.format("%Y-%m-%d %H:%M:%S.%f").to_string()
    );
    // println!("{}", start_time);
    // println!("{}", end_time);
    // println!("{}", clickhouse_query);

    let clickhouse_result = clickhouse_client
        .query(clickhouse_query.as_str())
        .fetch_all()
        .await?;

    println!("Clickhouse Count: {:?}", clickhouse_result);

    // // Process the result
    // if let Some(row) = clickhouse_result.into_iter().next() {
    //     match row.get::<u64, _>(0) {
    //         Some(clickhouse_count) => {
    //             println!(
    //                 "Tenant: {}, MongoDB Count: {}, ClickHouse Count: {}",
    //                 tenant_config.name, expected_count, clickhouse_count
    //             );
    //         }
    //         None => {
    //             log::warn!(
    //                 "Failed to retrieve count from ClickHouse result for tenant {}",
    //                 tenant_config.name
    //             );
    //         }
    //     }
    // } else {
    //     log::warn!(
    //         "No count result returned from ClickHouse for tenant {}",
    //         tenant_config.name
    //     );
    // }

    Ok(())
}
