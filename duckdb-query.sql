-- Install and load required extensions
INSTALL aws;
LOAD aws;
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL parquet;
LOAD parquet;
CALL load_aws_credentials();

-- Update extensions to the latest version
UPDATE EXTENSIONS;
UPDATE EXTENSIONS (iceberg);

-- Force install the latest nightly build for Iceberg integration
FORCE INSTALL iceberg FROM core_nightly;

-- Create AWS credentials secret
DROP SECRET IF EXISTS glue_secret;
CREATE SECRET glue_secret (
    TYPE S3,
    KEY_ID 'XX',
    SECRET 'X+XX',
    REGION 'us-east-2'
);

-- Connect to your Iceberg catalog
DETACH IF EXISTS my_iceberg_catalog;
ATTACH '867098943567:s3tablescatalog/pyiceberg-blog-bucket' AS my_iceberg_catalog (
    TYPE ICEBERG,
    ENDPOINT_TYPE 'GLUE'
);

-- List available tables
SHOW ALL TABLES;

-- Basic query to fetch all data
SELECT * FROM my_iceberg_catalog.myblognamespace.customers_nested;

-- Advanced query using JSON extraction functions
SELECT
    customer_id,
    name,
    json_extract(contact_info, '$.email') AS email,
    json_extract(contact_info, '$.phone') AS phone,
    json_extract(contact_info, '$.address.street') AS street,
    json_extract(contact_info, '$.address.city') AS city,
    json_extract(contact_info, '$.address.zip') AS zip
FROM my_iceberg_catalog.myblognamespace.customers_nested;
