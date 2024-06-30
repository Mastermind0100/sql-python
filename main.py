import sqlite3
import json

db_path = "oasis_copy.db"
table_name = "visits"
columns_to_check = ["cause_loss_va_os", "cause_loss_va_od"]

def get_all_mappings() -> dict:
  with open("hash_map.json") as f:
    data = json.load(f)
  return data

def generate_insert_command(all_symptoms:list, mappings:dict, id:str) -> str:
  all_columns = ''
  res = ''
  print(all_symptoms)
  for symptom in all_symptoms:
    mapped_val = mappings[symptom]
    if mapped_val != "":
      all_columns += f'{mapped_val}_od=\'Yes\', '
  if all_columns != '':
    res = f"UPDATE {table_name} SET {all_columns[:-2]} WHERE id='{id}'"
    print(res)
  return res

def iterate_with_values(db_path, table_name, columns):
  mappings = get_all_mappings()
  conn = sqlite3.connect(db_path)
  conn.row_factory = sqlite3.Row

  sql_command_1 = f"SELECT ID, {columns[0]}, {columns[1]} FROM {table_name} WHERE {columns[0]} IS NOT '' AND {columns[1]} IS NOT ''"

  cursor = conn.cursor()
  cursor.execute(sql_command_1)

  cur2 = conn.cursor()

  for row in cursor:
    # yield row
    data = dict(row)
    id = data['id']
    list_of_symptoms = data['cause_loss_va_od'] + ',' + data['cause_loss_va_os']
    all_symptoms = list_of_symptoms.split(',')
    sql_command_2 = generate_insert_command(all_symptoms, mappings, id)
    if sql_command_2 != '':
      cur2.execute(sql_command_2)

  conn.close()

if __name__ == "__main__":
  iterate_with_values(db_path, table_name, columns_to_check)


