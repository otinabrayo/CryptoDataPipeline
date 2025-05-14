-- Data Definition Language

-- Create file format
CREATE OR REPLACE FILE FORMAT CRYPTO_DB.BRONZE.my_json_format TYPE = 'JSON';

-- Create staging table
CREATE OR REPLACE TABLE CRYPTO_DB.BRONZE.br_coin_gecko_stage_json (raw VARIANT);

-- Load data into stage table
COPY INTO CRYPTO_DB.BRONZE.br_coin_gecko_stage_json
FROM @MY_S3_CRYPTO_STAGE/coin_gecko/dumps/
FILE_FORMAT = (FORMAT_NAME = CRYPTO_DB.BRONZE.my_json_format);

-- Create target table
CREATE OR REPLACE TABLE CRYPTO_DB.BRONZE.crypto_gecko_data (
    key_id STRING,
    id STRING,
    name STRING,
    market_cap FLOAT,
    market_cap_change_24h FLOAT,
    content STRING,
    top_3_coins ARRAY,
    top_3_coins_id ARRAY,
    volume_24h FLOAT,
    coin_updated_at TIMESTAMP_TZ
);

-- Data Manipulation Language

CREATE OR REPLACE PROCEDURE CRYPTO_DB.BRONZE.br_load_crypto_gecko()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert parsed data into structured table
    INSERT INTO CRYPTO_DB.BRONZE.crypto_gecko_data
    SELECT
        raw:key_id::STRING AS key_id,
        raw:id::STRING AS id,
        raw:name::STRING AS name,
        raw:market_cap::FLOAT AS market_cap,
        raw:market_cap_change_24h::FLOAT AS market_cap_change_24h,
        raw:content::STRING AS content,
        raw:top_3_coins::ARRAY AS top_3_coins,
        raw:top_3_coins_id::ARRAY AS top_3_coins_id,
        raw:volume_24h::FLOAT AS volume_24h,
        raw:coin_updated_at::TIMESTAMP_TZ AS coin_updated_at
    FROM CRYPTO_DB.BRONZE.br_coin_gecko_stage_json;

    RETURN 'Bronze Layer Insert completed successfully.';
END;
$$;

CALL CRYPTO_DB.BRONZE.br_load_crypto_gecko();