-- Daily Closing Price Query
SELECT
   CAST(full_date AS TIMESTAMP) AS Date
  ,stock_etf
  ,ROUND(CAST(CLOSE AS DOUBLE), 2) AS CLOSE_price
FROM stock_information_data_pqt_table
WHERE stock_etf IN ($Stock_ETF)
ORDER BY stock_etf ASC


-- Daily Price Change Query
SELECT
   CAST(full_date AS TIMESTAMP) AS Date
  ,stock_etf
  ,CAST(CLOSE AS DOUBLE) - CAST(OPEN AS DOUBLE) AS Daily_Change
FROM stock_information_data_pqt_table
WHERE stock_etf IN ($Stock_ETF)
ORDER BY stock_etf ASC


-- Rate of Increase Over Time (Percentage) Query
WITH min_date AS 
(SELECT 
  CAST(full_date AS TIMESTAMP) AS Date, 
  st.stock_etf, 
  CAST(ROUND(CAST(st.CLOSE AS DOUBLE), 2) AS DECIMAL(10, 2)) AS min_price
FROM (
  SELECT
    MIN(CAST(full_date AS TIMESTAMP)) AS Date,
    stock_etf
  FROM stock_information_data_pqt_table
  WHERE stock_etf IN ($Stock_ETF) AND $__timeFilter(CAST(full_date AS TIMESTAMP))
  GROUP BY stock_etf
) ti 
LEFT JOIN stock_information_data_pqt_table st ON ti.Date = CAST(st.full_date AS TIMESTAMP) AND ti.stock_etf = st.stock_etf)
SELECT Date, stock_etf, increase_rate
FROM( 
  SELECT
      CAST(st2.full_date AS TIMESTAMP) AS Date,
      st2.stock_etf,
      CAST(ROUND(CAST(st2.CLOSE AS DOUBLE), 2) AS DECIMAL(10, 2)) AS daily_close,
      mi.min_price,
      ROUND((((CAST(st2.CLOSE AS DOUBLE) / mi.min_price) - 1) * 100), 1) AS increase_rate
  FROM stock_information_data_pqt_table st2
  LEFT JOIN min_date mi ON mi.stock_etf = st2.stock_etf
  WHERE st2.stock_etf IN ($Stock_ETF) AND $__timeFilter(CAST(st2.full_date AS TIMESTAMP)))


-- Last Open Day Volume Query
SELECT
   full_date,
   SUM(volume) AS total_Volume
FROM stock_information_data_pqt_table
WHERE stock_etf IN ($Stock_ETF)
GROUP BY full_date
HAVING full_date = (
   SELECT MAX(full_date)
   FROM stock_information_data_pqt_table
)
ORDER BY full_date ASC


-- Daily Volume in Million Query
SELECT
   CAST(full_date AS TIMESTAMP) AS Date
  ,stock_etf
  ,ROUND(CAST(volume AS Integer)/1000000,2) Daily_Volume_Million
FROM stock_information_data_pqt_table
WHERE stock_etf IN ($Stock_ETF)
ORDER BY stock_etf ASC


-- Daily Price Range
SELECT
   CAST(full_date AS TIMESTAMP) AS Date
  ,stock_etf
  ,CAST(High AS DOUBLE) - CAST(Low AS DOUBLE) AS Daily_Change
FROM stock_information_data_pqt_table
WHERE stock_etf IN ($Stock_ETF)
ORDER BY stock_etf ASC 