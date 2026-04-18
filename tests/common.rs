use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use bank_service::domain::{account_number::AccountNumber, owner::Owner};

pub struct TestDatabase {
    pool: PgPool,
    _container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
}

impl TestDatabase {
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn into_pool(self) -> PgPool {
        self.pool
    }
}

pub async fn setup() -> TestDatabase {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    let database_url = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to test database container");

    TestDatabase {
        pool,
        _container: container,
    }
}

pub async fn run_migrations(pool: &PgPool) {
    sqlx::migrate!()
        .run(pool)
        .await
        .expect("Failed to run migrations");
}

pub async fn create_test_account(
    pool: &PgPool,
    id: Uuid,
    number: &AccountNumber,
    owner: &Owner,
    balance: u64,
) {
    let (user_id, org_id) = match owner {
        Owner::User(uid) => (Some(uid.as_str().to_string()), None),
        Owner::Org(oid) => (None, Some(oid.as_str().to_string())),
    };

    let _ = sqlx::query(
        r#"
        INSERT INTO accounts (id, number, user_id, org_id, balance, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (number) DO NOTHING
        "#,
    )
    .bind(id)
    .bind(number.as_str())
    .bind(user_id)
    .bind(org_id)
    .bind(balance as i64)
    .execute(pool)
    .await
    .expect("Failed to create test account");
}

pub async fn get_account_balance(pool: &PgPool, number: &AccountNumber) -> u64 {
    let row = sqlx::query_scalar::<_, i64>(r#"SELECT balance FROM accounts WHERE number = $1"#)
        .bind(number.as_str())
        .fetch_one(pool)
        .await
        .expect("Failed to get account balance");

    row as u64
}

pub async fn cleanup(pool: PgPool) {
    let _ = sqlx::query("TRUNCATE TABLE idempotency_keys, transactions, accounts CASCADE")
        .execute(&pool)
        .await;

    pool.close().await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}
