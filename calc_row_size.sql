--/
create or replace script calc_row_size(schema_name, table_name) as 
	suc, res = pquery([[SELECT COLUMN_MAXSIZE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA=:s AND COLUMN_TABLE=:t ORDER BY COLUMN_ORDINAL_POSITION]], {s=schema_name, t=table_name})
	total = 0
	for i=1, #res do
		total = total + res[i][1]
	end
output('row size: '..str(total))
/

execute script calc_row_size('test_schema', 'some_table') with output;
