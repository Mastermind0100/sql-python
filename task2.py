import sqlite3 as sql

database_path = 'oasis_copy.db'
table_name = 'visits'
columns = ['general_anatomic_diagnosis', 'general_anatomic_diagnosis_remarks', 'etiological_diagnosis', 
           'etiological_diagnosis_infective', 'etiological_diagnosis_noninfective', 'etiological_diagnosis_masquerade']
selection_column = 'ocular_examination_eye'
empty_values = [None, '']

def main():
    conn = sql.connect(database_path)
    conn.row_factory = sql.Row
    cur = conn.cursor()
    cur2 = conn.cursor()

    for column in columns:
        column_od = column + '_od'
        column_os = column + '_os'
        sql_command = f"SELECT ID, {column}, {column_od}, {column_os}, {selection_column} FROM visits"
        cur.execute(sql_command)

        for row in cur:
            data = dict(row)
            id = data['id']
            sql_command_update = ''
            if data[column] not in empty_values:
                if data[selection_column] == 'OU':
                    if data[column_od] in empty_values and data[column_os] in empty_values:
                        sql_command_update = f'UPDATE visits SET {column_od}="{data[column]}", {column_os}="{data[column]}" WHERE id="{id}"'
                elif data[selection_column] == "OD":
                    if data[column_od] in empty_values:
                        sql_command_update = f'UPDATE visits SET {column_od}="{data[column]}" WHERE id="{id}"'
                elif data[selection_column] == "OS":
                    if data[column_os] in empty_values:
                        sql_command_update = f'UPDATE visits SET {column_os}="{data[column]}" WHERE id="{id}"'
            else:
                if data[column_os] in empty_values and data[column_od] in empty_values:
                    continue
                if data[column_od] == data[column_os]:
                    sql_command_update = f'UPDATE visits SET {column}="{data[column_od]}" WHERE id="{id}"'
                else:
                    sql_command_update = f'UPDATE visits SET {column}="MIXED" WHERE id="{id}"'

            if sql_command_update != '':
                print(sql_command_update)
                cur2.execute(sql_command_update)
                conn.commit()

    conn.close()

if __name__ == "__main__":
    main()