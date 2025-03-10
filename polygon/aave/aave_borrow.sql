CREATE TABLE IF NOT EXISTS aave.aave_borrow (   
    version text,
    transaction_type text,
    loan_type text,
    symbol text,
    contract_address bytea,
    borrower bytea,
    repayer bytea,
    liquidator bytea,
    amount numeric,
    usd_amount numeric,
    evt_tx_hash bytea,
    evt_index integer,
    evt_block_time timestamptz,
    evt_block_number numeric,
    PRIMARY KEY (evt_tx_hash, evt_index)
);

CREATE OR REPLACE FUNCTION aave.insert_aave_borrow(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_borrow (
      version,
      transaction_type,
      loan_type,
      symbol,
      contract_address,
      borrower,
      repayer,
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
      loan_type,
      erc20.symbol,
      borrow.contract_address,
      borrower,
      repayer,
      liquidator,
      amount / (10^erc20.decimals) AS amount,
      (amount/(10^p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    
FROM (
-- v2
SELECT 
    '2' AS version,
    'borrow' AS transaction_type,
    CASE 
    WHEN "borrowRateMode" = '1' THEN 'stable'
    WHEN "borrowRateMode" = '2' THEN 'variable'
    END AS loan_type,
    reserve AS contract_address,
    "user" AS borrower, -- wat is onBehalfOf?
    NULL::bytea as repayer,
    NULL::bytea AS liquidator,
    amount, -- moet decimals af
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v2."LendingPool_evt_Borrow" tr1
-- LEFT JOIN erc20.tokens erc -- voor symbol en decimals
-- ON tr1.reserve = erc.contract_address
--LIMIT 10
UNION ALL 
-- all repays
SELECT 
    '2' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    reserve AS contract_address,
    "user" AS borrower,
    repayer AS repayer,
    NULL::bytea AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v2."LendingPool_evt_Repay"
--LIMIT 10
UNION ALL 
-- liquidation
SELECT 
    '2' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    "debtAsset" AS contract_address,
    "user" AS borrower,
    liquidator AS repayer,
    liquidator AS liquidator,
    - "debtToCover" AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v2."LendingPool_evt_LiquidationCall"
UNION ALL 

-- v3
SELECT 
    '3' AS version,
    'borrow' AS transaction_type,
    CASE 
    WHEN "interestRateMode" = '1' THEN 'stable'
    WHEN "interestRateMode" = '2' THEN 'variable'
    END AS loan_type,
    reserve AS contract_address,
    "user" AS borrower, -- wat is onBehalfOf?
     NULL::bytea as repayer,
    NULL::bytea AS liquidator,
    amount, -- moet decimals af
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Borrow" tr1
UNION ALL 
-- all repays
SELECT 
    '3' AS version,
    'repay' AS transaction_type,
    NULL AS loan_type,
    reserve AS contract_address,
    "user" AS borrower,
    repayer AS repayer,
    NULL::bytea AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Repay"

UNION ALL 
-- liquidation
SELECT 
    '2' AS version,
    'borrow_liquidation' AS transaction_type,
    NULL AS loan_type,
    "debtAsset" AS contract_address,
    "user" AS borrower,
    liquidator AS repayer,
    liquidator AS liquidator,
    - "debtToCover" AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_LiquidationCall"

) borrow
LEFT JOIN erc20."tokens" erc20
    ON borrow.contract_address = erc20.contract_address
LEFT JOIN prices.usd p 
    ON p.minute = date_trunc('minute', borrow.evt_block_time) 
    AND p.contract_address = borrow.contract_address
WHERE borrow.evt_block_time >= start_ts
AND borrow.evt_block_time < end_ts
AND borrow.evt_block_number >= start_block
AND borrow.evt_block_number < end_block 
    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_aave_borrow(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_borrow
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', $$
    SELECT aave.insert_aave_borrow(
        (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.aave_borrow),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM polygon.blocks WHERE time < (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.aave_borrow)),
        (SELECT MAX(number) FROM polygon.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
