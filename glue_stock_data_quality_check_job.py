import sys
import awswrangler as wr

# this check duplicate column 
# if any there a duplicate of stock + date, then there should be a row with rn bigger than 1
NULL_DQ_CHECK = f"""
SELECT 
    COUNT(*) AS duplicate_row
FROM "data_engineering_project_database"."stock_information_data_pqt_table"
WHERE rank_number > 1
;
"""

# run the quality check
df = wr.athena.read_sql_query(sql=NULL_DQ_CHECK, database="data_engineering_project_database")

# exit if we get a result > 0
# else, the check was successful
if df['duplicate_row'][0] > 0:
    sys.exit('Results returned. Quality check failed.')
else:
    print('Quality check passed.')
