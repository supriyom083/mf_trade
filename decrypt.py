from extract_avg_bank_balance import idbi_avg_balance_check, get_db_conn,get_day,get_account_info
import sys
from mysql.connector import Error



for k in get_account_info():
    conn = get_db_conn()
    for j in idbi_avg_balance_check(k[0], k[1]):
        sql='insert into bank_account_check(account_holder_name,\
        account_number,avg_bal_mnthly,\
        logged_date) values(%s,%s,%s,%s)'

        cursor_ins=conn.cursor()
        try:
            cursor_ins.execute(sql, (j[1], j[2], float(j[5].replace(',', '')), j[6]))
        except Error as error:
            cursor_ins.close()
            #conn.close()
            print('DB related issue occur.all connection closed.')
            print(error)
            #sys.exit(99)
        conn.commit()
        print(j)
    cursor_ins.close()
    conn.close()
sys.exit(1)