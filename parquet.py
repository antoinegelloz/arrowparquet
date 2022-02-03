import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df1 = pd.DataFrame({'values': [0]})
table1 = pa.Table.from_pandas(df1)
pq.write_table(table1, 'pass.parquet')

df2 = pd.DataFrame({'values': ["string"]})
table2 = pa.Table.from_pandas(df2)
pq.write_table(table2, 'fail.parquet')
