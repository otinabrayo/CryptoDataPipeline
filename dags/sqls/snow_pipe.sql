-- DROP PIPE IF EXISTS CRYPTO_DB.BRONZE.crypto_gecko_pipe;

CREATE OR REPLACE PIPE CRYPTO_DB.BRONZE.crypto_gecko_pipe
AUTO_INGEST = TRUE
AS
COPY INTO CRYPTO_DB.BRONZE.crypto_gecko_data
FROM (
    SELECT
        $1:key_id::STRING,
        $1:id::STRING,
        $1:name::STRING,
        $1:market_cap::FLOAT,
        $1:market_cap_change_24h::FLOAT,
        $1:content::STRING,
        $1:top_3_coins::ARRAY,
        $1:top_3_coins_id::ARRAY,
        $1:volume_24h::FLOAT,
        $1:coin_updated_at::TIMESTAMP_TZ
    FROM @MY_S3_CRYPTO_STAGE/coin_gecko/dumps/
    (FILE_FORMAT => my_json_format)
)

-- SELECT SYSTEM$PIPE_STATUS('CRYPTO_GECKO_PIPE');

-- SHOW PIPES;
