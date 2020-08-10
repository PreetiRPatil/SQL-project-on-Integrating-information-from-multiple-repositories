/***********  Stored Procedure which takes the global query and temporary table name as  input strings and generate sub queries & temporary tables that contain the 
results for each submitted subquery to each local database  & gives final output****************/

set serveroutput on;
show errors;
CREATE OR REPLACE PROCEDURE AUTO_CODE_GENR (in_str IN VARCHAR, tab_nm in varchar) 
 AS 
l_row_val               number;
l_column_value          varchar2(255); 
l_string_build          varchar2(3600); 
l_string_build_trim     varchar2(3600); 
l_cnt_clm               number;
l_cnt_tbl               number;
l_clm_nm                varchar2(255); 
l_tbl_nm                varchar2(255); 
l_sql_stm               varchar2(3600); 
l_db_val                number; 
l_sql_stm_tbl           varchar2(255); 
l_string_final          varchar2(3600); 
l_string_prv            varchar2(3600); 
l_str_vld               number;
sql1                    varchar2(3600); 
begin 

select count(*) into l_db_val
from all_tab_columns
where table_name='METADATA_TBL' 
and column_name like '%DB__COLUMN_NAME%';

for j in 1..l_db_val  
LOOP 
for i in (
select rownum, UPPER(column_value) AS column_value 
  from table(regex_substr(
(with str as (
select regexp_substr(in_str,'[^,]+', 1, level) as str_y from dual
    connect by regexp_substr(in_str, '[^,]+', 1, level) is not null
) 
select listagg(str_y, ' ')  within group (order by rownum) as str_d from str
)
  ))
        )
loop 
if trim(i.column_value) in ('SELECT', 'FROM', 'WHERE', '=', '<>', '!=', '<=', '>=', 'AND', 'OR', 'NOT', 'NOT LIKE', 'LIKE', 'IN', 'NOT IN')
then 
                if trim(i.column_value) IN ('FROM', '=', '<>', '!=', '<=', '>=', 'LIKE', 'IN')
                then 
                SELECT SUBSTR(l_string_build, 0, LENGTH(l_string_build) - 1) into l_string_build_trim FROM dual;
                l_string_build  := l_string_build_trim ||' '||  i.column_value ||' '; 
                else 
                l_string_build  := l_string_build ||' '||  i.column_value ||' ';  
                end if; 
ELSIF  i.column_value like ('''%') 
then 
        l_string_build  := l_string_build ||  i.column_value; 
ELSE 
SELECT count(*) into l_cnt_clm FROM METADATA_TBL WHERE upper(cnl_column_name) = i.column_value;
SELECT count(*) into l_cnt_tbl FROM METADATA_TBL WHERE upper(cnl_table_name) = TRIM(i.column_value); 
  --dbms_output.put_line ('Table_count:  ' || l_cnt_tbl); 
        if l_cnt_clm = 1 
        then 
        l_sql_stm := 'select nvl (' || 'db'||j ||'_column_name, ''999'')   from METADATA_TBL WHERE upper(cnl_column_name) = ' ||''''|| i.column_value || ''''; 
     --   dbms_output.put_line (l_sql_stm);
        execute immediate l_sql_stm into l_clm_nm; 
         l_string_build  := l_string_build || ' ' ||  l_clm_nm || ',';                        
        end if; 
        if l_cnt_tbl >= 1
        then   
        l_sql_stm_tbl := 'select distinct (nvl (' || 'db'||j ||'_table_name, ''999''))   from METADATA_TBL WHERE upper(cnl_table_name) = ' ||''''|| i.column_value || ''''; 
        -- dbms_output.put_line (l_sql_stm_tbl);
        execute immediate l_sql_stm_tbl into l_tbl_nm; 
      --  dbms_output.put_line (l_sql_stm_tbl);
        l_string_build  := l_string_build || ' ' ||  l_tbl_nm; 
        end if; 
        if l_cnt_clm = 0 and l_cnt_tbl = 0
        then 
        l_string_build  := l_string_build ||' '||  i.column_value;
     --   DBMS_OUTPUT.PUT_LINE ( 'CLM_VAL:   ' || i.column_value);
        end if;  
end if; 
end loop;
l_string_final := l_string_build;
l_str_vld := INSTR(l_string_final, '999', 1, 2);
if j = 1 then 
dbms_output.put_line ('Global String:  ' || CHR(10) || in_str || ';'); 
end if; 
if l_str_vld >= 1 then l_string_final := null;
else 
sql1 :=  'create table ' || tab_nm ||j || ' as ' || l_string_final; 
dbms_output.put(CHR(10));
dbms_output.put_line ('Subquery Statement DB' ||j|| ':  ' || CHR(10) || l_string_final||';'); 
dbms_output.put(CHR(10));
dbms_output.put_line ('Create Table Statement DB' ||j|| ':  ' || CHR(10) ||sql1||';');
dbms_output.put(CHR(10));
--execute immediate sql1;

if j = 1 then 
l_string_prv := l_string_final;
elsif j = 2 then  
l_string_prv := l_string_prv || ' Union ' || CHR(10) ||  l_string_final; 
else   
l_string_prv := l_string_prv || ' Union ' || CHR(10) ||  l_string_final; 
end if;


end if;
l_string_build := null; 
end loop; 
dbms_output.put(CHR(10));
dbms_output.put_line ('Final Query to all DBs:  ' || CHR(10) || l_string_prv||';');
dbms_output.put(CHR(10));
end;
