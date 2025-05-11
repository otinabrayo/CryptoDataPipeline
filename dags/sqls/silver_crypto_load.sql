-- Data Definition Language

-- Create target table
CREATE OR REPLACE TABLE CRYPTO_DB.SILVER.crypto_gecko_data (
    key_id STRING,
    id STRING,
    name STRING,
    market_cap FLOAT,
    market_cap_change_24h FLOAT,
    content STRING,
    top_3_coins ARRAY,
    top_3_coins_id ARRAY,
    volume_24h FLOAT,
    updated_at STRING
);

-- Data Manipulation Language

CREATE OR REPLACE PROCEDURE CRYPTO_DB.SILVER.sl_load_crypto_gecko()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert parsed data into structured table
    INSERT INTO CRYPTO_DB.SILVER.crypto_gecko_data
    SELECT
        -- RANK() OVER( ORDER BY key_id) comp_key,
        SUBSTR(key_id, 1, 8) AS key_id,
        REPLACE(id, '-', ' ') AS id,
        TRANSLATE(name, '-,.', '') AS name,
        ROUND(market_cap, 2) AS market_cap,
        ROUND(market_cap_change_24h, 4) AS market_cap_change_24h,
        CASE
            WHEN TRIM(content) = '' THEN 'unknown'
            ELSE content
        END AS content,
        TOP_3_COINS,
        TOP_3_COINS_ID,
        ROUND(volume_24h, 2) AS volume_24h,
        TO_CHAR(coin_updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at
    FROM CRYPTO_DB.BRONZE.CRYPTO_GECKO_DATA;
    
    COMMIT;
    RETURN 'Silver Layer Insert completed successfully.';
END;
$$;

CALL CRYPTO_DB.SILVER.sl_load_crypto_gecko();