drop table if exists st23052706__staging.transactions;
create table st23052706__staging.transactions(
    operation_id varchar(1000) not null,
    account_number_from integer not null,
    account_number_to integer not null,
    currency_code integer not null,
    country varchar(100) not null,
    status varchar(100) not null,
    transaction_type varchar(100) not null,
    amount integer not null,
    transaction_dt timestamp(3) not null,
    unique(operation_id, status) enabled
)
order by transaction_dt,
    operation_id,
    status
segmented by hash(operation_id, status) all nodes
partition by trunc(transaction_dt, 'mm')::date;

create projection st23052706__staging.transactions_b0
/*+basename(transactions),createtype(p)*/
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
) as
select transactions.operation_id,
    transactions.account_number_from,
    transactions.account_number_to,
    transactions.currency_code,
    transactions.country,
    transactions.status,
    transactions.transaction_type,
    transactions.amount,
    transactions.transaction_dt
from st23052706__staging.transactions
order by transactions.transaction_dt,
    transactions.operation_id,
    transactions.status
segmented by hash(transactions.operation_id, transactions.status) all nodes offset 0;
select mark_design_ksafe(1);

drop table if exists st23052706__staging.currencies;
create table st23052706__staging.currencies(
    currency_code integer not null,
    currency_code_with integer not null,
    date_update date not null,
    currency_with_div decimal(3, 2) not null,
    unique(currency_code, currency_code_with, date_update) enabled
)
order by currency_code,
    currency_code_with,
    date_update
segmented by hash(currency_code, currency_code_with, date_update) all nodes;

create projection st23052706__staging.currencies_b0
/*+basename(currencies),createtype(p)*/
(
    currency_code,
    currency_code_with,
    date_update,
    currency_with_div
) as
select currencies.currency_code,
    currencies.currency_code_with,
    currencies.date_update,
    currencies.currency_with_div
from st23052706__staging.currencies
order by currencies.currency_code,
    currencies.currency_code_with,
    currencies.date_update
segmented by hash(
    currencies.currency_code,
    currencies.currency_code_with,
    currencies.date_update
) all nodes offset 0;
select mark_design_ksafe(1);

drop table if exists st23052706__dwh.global_metrics;
create table st23052706__dwh.global_metrics(
    date_update date not null,
    currency_from integer not null,
    amount_total decimal(16, 2) not null,
    cnt_transactions integer not null,
    avg_transactions_per_account decimal(12, 2) not null,
    cnt_accounts_make_transactions integer not null
)
order by date_update,
    currency_from
segmented by hash(date_update, currency_from) all nodes;