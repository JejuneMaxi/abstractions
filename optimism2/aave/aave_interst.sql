CREATE TABLE IF NOT EXISTS aave.aave_interest (   
    day timestamptz,
    symbol text,
    reserve bytea,
    deposit_apy numeric,
    stable_borrow_apy numeric,
    variable_borrow_apy numeric,
    daily_deposit_apr numeric,
    daily_stable_borrow_apr numeric,
    daily_variable_borrow_apr numeric,
    PRIMARY KEY (reserve, day)
    
);

CREATE OR REPLACE FUNCTION aave.insert_aave_interest(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_interest (
    day,
    symbol,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    daily_deposit_apr,
    daily_stable_borrow_apr,
    daily_variable_borrow_apr
    )
    ((SELECT
    
    day,
    symbol,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    deposit_apr AS daily_deposit_apr,
    stable_borrow_apr AS daily_stable_borrow_apr,
    variable_borrow_apr AS daily_variable_borrow_apr
FROM (
SELECT 
    gs.day,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    deposit_apr,
    stable_borrow_apr,
    variable_borrow_apr
FROM (
SELECT
    day,
    lead(day, 1, DATE_TRUNC('day',now() + '1 day'::interval) ) OVER (PARTITION BY "reserve"
                            ORDER BY day asc) AS next_day,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    -- DENK FF NA OF DE LOGICA KLOPT DAT IK HET ZO CONVERT NAAR APR
    ((1+deposit_apy)^(1.0/365.0)-1) AS deposit_apr, --convert apy to daily apr
    ((1+stable_borrow_apy)^(1.0/365.0)-1) AS stable_borrow_apr,
    ((1+variable_borrow_apy)^(1.0/365.0)-1) AS variable_borrow_apr
FROM (
SELECT -- *
    date_trunc('day', evt_block_time) AS day,
    reserve,
    AVG("liquidityRate" / 1e27) AS deposit_apy,
    AVG("stableBorrowRate" / 1e27) AS stable_borrow_apy,
    AVG("variableBorrowRate" /1e27) AS variable_borrow_apy--,
    -- "liquidityIndex" /1e27 AS liquidity_index,
    -- "variableBorrowIndex" / 1e27 AS variable_borrow_index,
    -- contract_address
FROM aave_v3."Pool_evt_ReserveDataUpdated"
--WHERE reserve = '\x0d8775f648430679a709e98d2b0cb6250d2887ef'
GROUP BY 1, 2
) o
) day
INNER JOIN 
(SELECT generate_series('2020-01-01', NOW(), '1 day') AS day) gs -- to select gap days
ON day.day <= gs.day
AND gs.day < day.next_day
) i
LEFT JOIN (
    SELECT 
        contract_address, 
        tokens.decimals,
        CASE WHEN (symbol = 'ETH'::text) THEN 'WETH'::text ELSE symbol
        END AS "symbol"
    FROM erc20.tokens
) erc20tokens
ON i.reserve = erc20tokens.contract_address

    
    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_aave_interest(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_interest
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
