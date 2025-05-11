-- Data Definition Language

-- Create target table
CREATE OR REPLACE TABLE CRYPTO_DB.GOLD.crypto_gecko_data (
    company_number INT,
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

CREATE OR REPLACE PROCEDURE CRYPTO_DB.GOLD.gl_load_crypto_gecko()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert parsed data into structured table
    INSERT INTO CRYPTO_DB.GOLD.crypto_gecko_data
    SELECT
        RANK() OVER( ORDER BY key_id) AS company_number,
        key_id,
        id,
        name,
        market_cap,
        market_cap_change_24h,
        content,
        top_3_coins,
        top_3_coins_id,
        volume_24h,
        updated_at
    FROM CRYPTO_DB.SILVER.crypto_gecko_data
    WHERE market_cap IS NOT NULL
    AND market_cap_change_24h IS NOT NULL
    AND top_3_coins != []
    AND top_3_coins_id != []
    AND content != 'unknown'
    AND volume_24h IS NOT NULL
    AND updated_at IS NOT NULL;

    COMMIT;
    RETURN 'Gold Layer Insert completed successfully.';
END;
$$;

CALL CRYPTO_DB.GOLD.gl_load_crypto_gecko();