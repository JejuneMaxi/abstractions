CREATE TABLE IF NOT EXISTS aave.aave_deposit (   
    version text,
    transaction_type text,
    symbol text,
    contract_address bytea,
    borrower bytea,
    "to" bytea,
    liquidator bytea,
    amount numeric,
    usd_amount numeric,
    evt_tx_hash bytea,
    evt_index integer,
    evt_block_time timestamptz,
    evt_block_number numeric
);

CREATE OR REPLACE FUNCTION aave.insert_aave_deposit(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_deposit (
      version,
      transaction_type,
      symbol,
      contract_address,
      borrower,
      "to",
      liquidator,
      amount,
      usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
      )
    ((SELECT  
      
      version,
      transaction_type,
      erc20.symbol,
      deposit.contract_address,
      borrower,
      "to",
      liquidator,
      amount / (10^erc20.decimals) AS amount,
      (amount/(10^p.decimals)) * median_price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    
FROM (

SELECT 
    '3' AS version,
    'deposit' AS transaction_type,
    reserve AS contract_address,
    "user" AS borrower, -- wat is onBehalfOf?
    NULL::bytea as "to",
    NULL::bytea AS liquidator,
    amount, -- moet decimals af
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Supply" tr1
UNION ALL 
-- all withdrawals
SELECT 
    '3' AS version,
    'withdrawn' AS transaction_type,
    reserve AS contract_address,
    "user" AS borrower,
    "to" AS "to",
    NULL::bytea AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Withdraw"
UNION ALL 
-- liquidation
SELECT 
    '3' AS version,
    'deposit_liquidation' AS transaction_type,
    "collateralAsset" AS contract_address,
    "user" AS borrower,
    liquidator AS "to",
    liquidator AS liquidator,
    - "liquidatedCollateralAmount" AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_LiquidationCall"
) deposit
LEFT JOIN erc20."tokens" erc20
    ON deposit.contract_address = erc20.contract_address
LEFT JOIN prices."approx_prices_from_dex_data" p
    ON p.hour = date_trunc('hour', deposit.evt_block_time) 
    AND p.contract_address = deposit.contract_address

    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_aave_deposit(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_deposit
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', $$
    SELECT aave.insert_aave_borrow(
        (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.aave_borrow),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM optimism.blocks WHERE time < (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.aave_borrow)),
        (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
