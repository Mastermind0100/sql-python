import sqlite3 as sql
import json

db_path = "oasis.db"
table_name = "visits"
columns_to_check = ["cause_loss_va_os", "cause_loss_va_od"]

with open("hash_map.json") as file:
    hash_map = json.load(file)

if __name__ == "__main__":
    conn = sql.connect(db_path)
    conn.row_factory = sql.Row

    command = f"SELECT ID, {columns_to_check[0]}, {columns_to_check[1]} FROM {table_name} WHERE {columns_to_check[0]} IS NOT '' AND {columns_to_check[1]} IS NOT ''"

    cur = conn.cursor()
    cur.execute(command)

    all_symptoms = []

    for row in cur:
        data = dict(row)
        list_of_symptoms = data['cause_loss_va_od'] + ',' + data['cause_loss_va_os']
        all_symptoms.extend(list_of_symptoms.split(','))
    
    all_symptoms = list(set(all_symptoms))

    for symptom in all_symptoms:
        if symptom not in hash_map:
            hash_map[symptom] = ''

    with open('hash_map.json', 'w') as file:
        json.dump(hash_map, file, indent=4)
