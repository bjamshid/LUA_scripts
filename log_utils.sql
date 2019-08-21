create or replace script logger() as
    function create_log_table(schema_name, table_name)
		op_res = false
		cre_suc, cre_res = pquery([[create table if not exists ::s_name]]..'.'..[[::t_name(log_timestamp timestamp default current_timestamp, status integer, logged_msg varchar(8192),
									logged_statement varchar(16384))]], {s_name=schema_name, t_name=table_name})

		trunc_suc, trunc_res = pquery([[truncate table ::s_name]]..'.'..[[::t_name]], {s_name=schema_name, t_name=table_name})
		if cre_suc and trunc_suc then
			return true, cre_res
        else
		    return op_res, cre_res.error_message
        end
	end

	function insert_into_log_table(table_name, status, log_msg, log_stat)
		count_ok, count_res = pquery([[select count(*) from ::t_name]], {t_name=table_name})
		if count_ok then
			ins_ok, ins_res = pquery([[insert into ::t_name values(current_timestamp, 
			:stat, :msg, :log_st)]], {t_name=table_name, stat=status, msg=log_msg, log_st=log_stat})
            if not ins_ok then
                return ins_ok, ins_res.error_message
            else
                return ins_ok, 'inserted: '..log_msg
            end
		else
		    return false, count_res.error_message
        end
	end
/
