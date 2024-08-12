import sqlite3
import json

db_path = "oasis.db"
table_name = "visits"
columns_to_check = ["cause_loss_va_os", "cause_loss_va_od"]
new_columns = ['patient_attended', 'cataract_od', 'cataract_os', 'high_ocular_hypertension_od', 'high_ocular_hypertension_os', 'glaucoma_od', 'glaucoma_os', 
               'angles_od', 'angles_os', 'cystoid_macular_edema_od', 'cystoid_macular_edema_os', 'corneal_opacity_od', 'corneal_opacity_os', 
               'band_keratopathy_od', 'band_keratopathy_os', 'anterior_synechaie_od', 'anterior_synechaie_os', 'posterior_synechaie_od', 
               'posterior_synechaie_os', 'choroidal_neovascularisation_od', 'choroidal_neovascularisation_os', 'vitreous_detachment_od',
               'vitreous_detachment_os', 'epiretinal_membrane_od', 'epiretinal_membrane_os', 'retinal_scar_od', 'retinal_scar_os', 
               'retinal_vascular_occlusion_od', 'retinal_vascular_occlusion_os', 'macular_ischemia_od', 'macular_ischemia_os',
               'optic_atrophy_od', 'optic_atrophy_os', 'phthisis_od', 'phthisis_os']

def get_all_mappings() -> dict:
  with open("hash_map.json") as f:
    data = json.load(f)
  return data

def generate_insert_command(od_symptoms:list, os_symptoms:list, mappings:dict, id:str) -> str:
  all_columns = ''
  res = ''
  # print(od_symptoms, os_symptoms)
  for symptom in od_symptoms:
    if symptom not in mappings:
      continue
    mapped_vals = mappings[symptom]
    for mapped_val in mapped_vals:
      if mapped_val != "":
        all_columns += f'{mapped_val}_od=\'Yes\', '
    
  for symptom in os_symptoms:
    if symptom not in mappings:
      continue
    mapped_vals = mappings[symptom]
    for mapped_val in mapped_vals:
      if mapped_val != "":
        all_columns += f'{mapped_val}_os=\'Yes\', '
  
  if all_columns != '':
    res = f"UPDATE {table_name} SET {all_columns[:-2]} WHERE id='{id}'"
  return res

def iterate_with_values(db_path, table_name, columns):
  mappings = get_all_mappings()
  conn = sqlite3.connect(db_path)
  conn.row_factory = sqlite3.Row

  sql_command_1 = f"SELECT ID, {columns[0]}, {columns[1]} FROM {table_name} WHERE {columns[0]} IS NOT '' OR {columns[1]} IS NOT ''"

  cursor = conn.cursor()
  cursor.execute(sql_command_1)

  cur2 = conn.cursor()

  for row in cursor:
    # yield row
    data = dict(row)
    id = data['id']
    if id == '2ba60797-3d88-4ae8-9452-3894d6957f28':
      print("hello")
      print(data)
    od_symptoms = data['cause_loss_va_od'].split(',')
    os_symptoms = data['cause_loss_va_os'].split(',')
    sql_command_2 = generate_insert_command(od_symptoms, os_symptoms, mappings, id)

    if sql_command_2 != '':
      # print(sql_command_2)
      cur2.execute(sql_command_2)
      conn.commit()

  conn.close()

def iterate_with_values_v2(db_path, table_name, columns):
  conn = sqlite3.connect(db_path)
  conn.row_factory = sqlite3.Row

  sql_command_1 = f"SELECT ID, {', '.join(x for x in columns)} FROM {table_name}"

  cursor = conn.cursor()
  cursor.execute(sql_command_1)

  cursor2 = conn.cursor()

  for row in cursor:
    data = dict(row)
    id = data['id']
    patient_attended = data['patient_attended']
    positive_complications:list[str] = []
    od_complication = False
    os_complication = False
    for key in data.keys():
      if data[key] == 'Yes':
        positive_complications.append(key)
    for complication in positive_complications:
      if complication.endswith('od'):
        od_complication = True
      if complication.endswith('os'):
        os_complication = True
    
    if od_complication and os_complication:
      sql_command_2 = f"UPDATE {table_name} SET complications_eye='OU' WHERE id='{id}'"
    elif od_complication and not os_complication:
      sql_command_2 = f"UPDATE {table_name} SET complications_eye='OD' WHERE id='{id}'"
    elif os_complication and not od_complication:
      sql_command_2 = f"UPDATE {table_name} SET complications_eye='OS' WHERE id='{id}'"
    else:
      if patient_attended == 'Yes':
        sql_command_2 = f"UPDATE {table_name} SET complications_eye='None' WHERE id='{id}'"
      else:
        sql_command_2 = f"UPDATE {table_name} SET complications_eye=NULL WHERE id='{id}'"
    
    print(sql_command_2)
    # print(os_complication, od_complication)
    cursor2.execute(sql_command_2)
    conn.commit()

  conn.close()


if __name__ == "__main__":
  iterate_with_values(db_path, table_name, columns_to_check)
  iterate_with_values_v2(db_path, table_name, new_columns)
