use std::sync::Arc;

use bank_service::{
    application::{
        deposit::{DepositInput, DepositUseCase},
        ports::{BalanceCachePort, EventPublisher, MetricsPort},
        transaction_manager::TransactionManager,
        transfer::{TransferInput, TransferUseCase},
        withdraw::{WithdrawInput, WithdrawUseCase},
    },
    domain::{account_number::AccountNumber, amount::Amount, owner::Owner, user_id::UserId},
    infrastructure::{
        database::transaction::Manager,
        observability::metrics::Metrics,
        repositories::{
            sqlx_account_repository::SqlxAccountRepository,
            sqlx_account_tx_repository::SqlxAccountTxRepository,
            sqlx_idempotency_repository::SqlxIdempotencyRepository,
            sqlx_transaction_repository::SqlxTransactionRepository,
            sqlx_transaction_write_repository::SqlxTransactionWriteRepository,
        },
    },
};
use uuid::Uuid;

mod common;
mod test_utils;
use test_utils::create_test_balance_cache;

#[tokio::test]
async fn test_concurrent_deposits() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_number = AccountNumber::new("test-acc-1").unwrap();
    let account_id = Uuid::new_v4();

    common::create_test_account(pool, account_id, &account_number, &owner, 0).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(DepositUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let num_tasks = 10;
    let deposit_amount = 10;
    let mut handles = vec![];

    for _ in 0..num_tasks {
        let uc = use_case.clone();
        let acc_num = account_number.clone();

        let handle = tokio::spawn(async move {
            let input = DepositInput {
                account_number: acc_num,
                amount: Amount::new(deposit_amount).unwrap(),
                idempotency_key: None,
            };
            uc.execute(input).await
        });
        handles.push(handle);
    }

    let results: Vec<Result<Result<_, _>, _>> = futures_util::future::join_all(handles).await;

    let success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();
    let expected_successes = num_tasks;

    assert_eq!(
        success_count, expected_successes,
        "All deposits should succeed"
    );

    let final_balance = common::get_account_balance(pool, &account_number).await;
    let expected_balance = (num_tasks as u64) * deposit_amount;
    assert_eq!(
        final_balance, expected_balance,
        "Final balance should equal sum of all deposits"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_concurrent_withdrawals() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_number = AccountNumber::new("test-acc-2").unwrap();
    let account_id = Uuid::new_v4();
    let initial_balance = 100;

    common::create_test_account(pool, account_id, &account_number, &owner, initial_balance).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(WithdrawUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let num_tasks = 5;
    let withdraw_amount = 10;
    let mut handles = vec![];

    for _ in 0..num_tasks {
        let uc = use_case.clone();
        let acc_num = account_number.clone();

        let handle = tokio::spawn(async move {
            let input = WithdrawInput {
                account_number: acc_num,
                amount: Amount::new(withdraw_amount).unwrap(),
                idempotency_key: None,
            };
            uc.execute(input).await
        });
        handles.push(handle);
    }

    let results: Vec<Result<Result<_, _>, _>> = futures_util::future::join_all(handles).await;

    let success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    let final_balance = common::get_account_balance(pool, &account_number).await;
    let expected_balance = initial_balance - (success_count as u64 * withdraw_amount);
    assert_eq!(
        final_balance, expected_balance,
        "Final balance should reflect successful withdrawals"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_concurrent_transfers() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_a = AccountNumber::new("test-acc-a").unwrap();
    let account_b = AccountNumber::new("test-acc-b").unwrap();
    let initial_balance = 1000;

    common::create_test_account(pool, Uuid::new_v4(), &account_a, &owner, initial_balance).await;
    common::create_test_account(pool, Uuid::new_v4(), &account_b, &owner, initial_balance).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(TransferUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let num_tasks = 10;
    let transfer_amount = 10;
    let mut handles = vec![];

    for i in 0..num_tasks {
        let uc = use_case.clone();
        let from = if i % 2 == 0 {
            account_a.clone()
        } else {
            account_b.clone()
        };
        let to = if i % 2 == 0 {
            account_b.clone()
        } else {
            account_a.clone()
        };

        let handle = tokio::spawn(async move {
            let input = TransferInput {
                from_account_number: from,
                to_account_number: to,
                amount: Amount::new(transfer_amount).unwrap(),
                idempotency_key: None,
            };
            uc.execute(input).await
        });
        handles.push(handle);
    }

    let results: Vec<Result<Result<_, _>, _>> = futures_util::future::join_all(handles).await;
    let _success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    let balance_a = common::get_account_balance(pool, &account_a).await;
    let balance_b = common::get_account_balance(pool, &account_b).await;

    let total_balance = balance_a + balance_b;
    assert_eq!(
        total_balance,
        initial_balance * 2,
        "Total balance should be conserved"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_idempotency_duplicate_requests() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_number = AccountNumber::new("test-acc-idem").unwrap();

    common::create_test_account(pool, Uuid::new_v4(), &account_number, &owner, 0).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(DepositUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let idempotency_key = Some("unique-key-123".to_string());
    let amount = Amount::new(100).unwrap();

    let mut handles = vec![];
    for _ in 0..5 {
        let uc = use_case.clone();
        let acc_num = account_number.clone();
        let key = idempotency_key.clone();

        let handle = tokio::spawn(async move {
            let input = DepositInput {
                account_number: acc_num,
                amount,
                idempotency_key: key,
            };
            uc.execute(input).await
        });
        handles.push(handle);
    }

    let results: Vec<Result<Result<_, _>, _>> = futures_util::future::join_all(handles).await;
    let _success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    let final_balance = common::get_account_balance(pool, &account_number).await;
    assert_eq!(
        final_balance, 100,
        "Only one deposit should succeed with same idempotency key"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_deadlock_opposite_transfers() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_a = AccountNumber::new("test-deadlock-a").unwrap();
    let account_b = AccountNumber::new("test-deadlock-b").unwrap();
    let initial_balance = 1000;

    common::create_test_account(pool, Uuid::new_v4(), &account_a, &owner, initial_balance).await;
    common::create_test_account(pool, Uuid::new_v4(), &account_b, &owner, initial_balance).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(TransferUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let account_a_clone1 = account_a.clone();
    let account_b_clone1 = account_b.clone();
    let handle1 = {
        let uc = use_case.clone();
        tokio::spawn(async move {
            let input = TransferInput {
                from_account_number: account_a_clone1,
                to_account_number: account_b_clone1,
                amount: Amount::new(100).unwrap(),
                idempotency_key: None,
            };
            uc.execute(input).await
        })
    };

    let account_a_clone2 = account_a.clone();
    let account_b_clone2 = account_b.clone();
    let handle2 = {
        let uc = use_case.clone();
        tokio::spawn(async move {
            let input = TransferInput {
                from_account_number: account_b_clone2,
                to_account_number: account_a_clone2,
                amount: Amount::new(50).unwrap(),
                idempotency_key: None,
            };
            uc.execute(input).await
        })
    };

    let results: Vec<Result<Result<_, _>, _>> =
        futures_util::future::join_all(vec![handle1, handle2]).await;
    let success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    assert_eq!(success_count, 2, "Both opposite transfers should succeed");

    let balance_a = common::get_account_balance(pool, &account_a).await;
    let balance_b = common::get_account_balance(pool, &account_b).await;

    let expected_total = initial_balance * 2;
    let actual_total = balance_a + balance_b;
    assert_eq!(
        actual_total, expected_total,
        "Total balance should be conserved"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_concurrent_idempotent_transfers() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_a = AccountNumber::new("test-idem-a").unwrap();
    let account_b = AccountNumber::new("test-idem-b").unwrap();
    let initial_balance = 1000;

    common::create_test_account(pool, Uuid::new_v4(), &account_a, &owner, initial_balance).await;
    common::create_test_account(pool, Uuid::new_v4(), &account_b, &owner, initial_balance).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager,
        account_repo,
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo,
        None,
        Some(metrics as Arc<dyn MetricsPort>),
    ));
    let (cache, _container) = create_test_balance_cache().await;
    let use_case = Arc::new(TransferUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let num_tasks = 5;
    let transfer_amount = 10;
    let idempotency_key = "shared-idem-key".to_string();
    let mut handles = vec![];

    for _ in 0..num_tasks {
        let uc = use_case.clone();
        let from = account_a.clone();
        let to = account_b.clone();
        let key = Some(idempotency_key.clone());

        let handle = tokio::spawn(async move {
            let input = TransferInput {
                from_account_number: from,
                to_account_number: to,
                amount: Amount::new(transfer_amount).unwrap(),
                idempotency_key: key,
            };
            uc.execute(input).await
        });
        handles.push(handle);
    }

    let results: Vec<Result<Result<_, _>, _>> = futures_util::future::join_all(handles).await;
    let _success_count = results
        .iter()
        .filter(|r: &&Result<Result<_, _>, _>| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    let balance_a = common::get_account_balance(pool, &account_a).await;
    let balance_b = common::get_account_balance(pool, &account_b).await;

    assert_eq!(
        balance_a,
        initial_balance - transfer_amount,
        "Only one transfer should deduct from account A"
    );
    assert_eq!(
        balance_b,
        initial_balance + transfer_amount,
        "Only one transfer should add to account B"
    );

    common::cleanup(db.into_pool()).await;
}

#[tokio::test]
async fn test_concurrent_mixed_operations() {
    let db = common::setup().await;
    let pool = db.pool();
    common::run_migrations(pool).await;

    let account_repo = Arc::new(SqlxAccountRepository::new(pool.clone()));
    let tx_repo = Arc::new(SqlxTransactionRepository::new(pool.clone()));
    let idem_repo = Arc::new(SqlxIdempotencyRepository::new(pool.clone()));
    let db_tx_manager = Manager::new(pool.clone());

    let owner = Owner::User(UserId::new("test-user").unwrap());
    let account_a = AccountNumber::new("test-mixed-a").unwrap();
    let account_b = AccountNumber::new("test-mixed-b").unwrap();
    let initial_balance = 500;

    common::create_test_account(pool, Uuid::new_v4(), &account_a, &owner, initial_balance).await;
    common::create_test_account(pool, Uuid::new_v4(), &account_b, &owner, initial_balance).await;

    let metrics = Arc::new(Metrics::new().unwrap());
    let tx_manager = Arc::new(TransactionManager::new(
        db_tx_manager.clone(),
        account_repo.clone(),
        Arc::new(SqlxAccountTxRepository),
        Arc::new(SqlxTransactionWriteRepository),
        idem_repo.clone(),
        None,
        Some(metrics.clone() as Arc<dyn MetricsPort>),
    ));

    let (cache, _container) = create_test_balance_cache().await;
    let deposit_uc = Arc::new(DepositUseCase::new(
        Arc::new(TransactionManager::new(
            db_tx_manager.clone(),
            account_repo.clone(),
            Arc::new(SqlxAccountTxRepository),
            Arc::new(SqlxTransactionWriteRepository),
            idem_repo.clone(),
            None,
            Some(metrics.clone() as Arc<dyn MetricsPort>),
        )),
        Arc::new(cache.clone()) as Arc<dyn BalanceCachePort>,
    ));

    let (cache, _container) = create_test_balance_cache().await;
    let withdraw_uc = Arc::new(WithdrawUseCase::new(
        Arc::new(TransactionManager::new(
            db_tx_manager.clone(),
            account_repo.clone(),
            Arc::new(SqlxAccountTxRepository),
            Arc::new(SqlxTransactionWriteRepository),
            idem_repo.clone(),
            None,
            Some(metrics.clone() as Arc<dyn MetricsPort>),
        )),
        Arc::new(cache.clone()) as Arc<dyn BalanceCachePort>,
    ));

    let (cache, _container) = create_test_balance_cache().await;
    let transfer_uc = Arc::new(TransferUseCase::new(
        tx_manager,
        Arc::new(cache) as Arc<dyn BalanceCachePort>,
    ));

    let num_deposits = 3;
    let num_withdrawals = 3;
    let num_transfers = 4;
    let deposit_amount = 50;
    let withdraw_amount = 30;
    let transfer_amount = 20;
    let mut deposit_handles = vec![];
    let mut withdraw_handles = vec![];
    let mut transfer_handles = vec![];

    for i in 0..num_deposits {
        let uc = deposit_uc.clone();
        let acc_num = account_a.clone();
        let handle = tokio::spawn(async move {
            let input = DepositInput {
                account_number: acc_num,
                amount: Amount::new(deposit_amount).unwrap(),
                idempotency_key: Some(format!("deposit-{}", i)),
            };
            uc.execute(input).await
        });
        deposit_handles.push(handle);
    }

    for i in 0..num_withdrawals {
        let uc = withdraw_uc.clone();
        let acc_num = account_a.clone();
        let handle = tokio::spawn(async move {
            let input = WithdrawInput {
                account_number: acc_num,
                amount: Amount::new(withdraw_amount).unwrap(),
                idempotency_key: Some(format!("withdraw-{}", i)),
            };
            uc.execute(input).await
        });
        withdraw_handles.push(handle);
    }

    for i in 0..num_transfers {
        let uc = transfer_uc.clone();
        let from = if i % 2 == 0 {
            account_a.clone()
        } else {
            account_b.clone()
        };
        let to = if i % 2 == 0 {
            account_b.clone()
        } else {
            account_a.clone()
        };
        let handle = tokio::spawn(async move {
            let input = TransferInput {
                from_account_number: from,
                to_account_number: to,
                amount: Amount::new(transfer_amount).unwrap(),
                idempotency_key: Some(format!("transfer-{}", i)),
            };
            uc.execute(input).await
        });
        transfer_handles.push(handle);
    }

    let _deposit_results = futures_util::future::join_all(deposit_handles).await;
    let _withdraw_results = futures_util::future::join_all(withdraw_handles).await;
    let _transfer_results = futures_util::future::join_all(transfer_handles).await;

    let balance_a = common::get_account_balance(pool, &account_a).await;
    let balance_b = common::get_account_balance(pool, &account_b).await;

    let expected_total = initial_balance * 2 + (num_deposits as u64 * deposit_amount);
    let expected_withdrawals = num_withdrawals as u64 * withdraw_amount;
    let expected_transfers_out_a = (num_transfers / 2) as u64 * transfer_amount;
    let expected_transfers_in_a = (num_transfers / 2) as u64 * transfer_amount;

    let expected_balance_a = initial_balance + (num_deposits as u64 * deposit_amount)
        - expected_withdrawals
        - expected_transfers_out_a
        + expected_transfers_in_a;

    let expected_balance_b = initial_balance + expected_transfers_out_a - expected_transfers_in_a;

    let actual_total = balance_a + balance_b + expected_withdrawals;
    assert_eq!(
        actual_total, expected_total,
        "Total balance (including withdrawn amounts) should be conserved"
    );

    assert_eq!(
        balance_a, expected_balance_a,
        "Account A should have expected balance"
    );
    assert_eq!(
        balance_b, expected_balance_b,
        "Account B should have expected balance"
    );

    common::cleanup(db.into_pool()).await;
}
