# 1. Create the table.
CREATE TABLE orders (created_date TIMESTAMP, verified_date TIMESTAMP, order_number BIGINT, country STRING, username STRING, product STRING, status STRING) PARTITIONED BY (ingest_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' TBLPROPERTIES("skip.header.line.count"="1");

# 2. Load data from a file for the first day.
LOAD DATA INPATH 'file:///home/hadoop/data/sales/20191101/data.csv' INTO TABLE orders PARTITION (ingest_date='20191101');

# 3. Load data from a file for the second day.
LOAD DATA INPATH 'file:///home/hadoop/data/sales/20191102/data.csv' INTO TABLE orders PARTITION (ingest_date='20191102');

# 4. Get latest data for every order_number.
WITH tm AS (SELECT order_number, max(ingest_date) as max_ingest_date FROM orders GROUP BY order_number) SELECT o.created_date, o.verified_date, o.order_number, o.country, o.username, o.product, o.status FROM orders o CROSS JOIN tm ON o.order_number = tm.order_number AND o.ingest_date = tm.max_ingest_date ORDER BY order_number ASC;

# 5. Get every step of the order number.
SELECT * FROM orders where order_number = 2;