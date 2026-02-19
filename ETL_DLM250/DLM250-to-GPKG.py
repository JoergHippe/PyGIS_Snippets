import os
import subprocess
import sqlite3
import pandas as pd
import argparse
from datetime import datetime

def build_sql_query(src_file, filter_sql, lookups_df):
    select_parts = ["*"]
    
    # OBJART Klartext
    subset_obj = lookups_df[lookups_df['kategorie'] == 'OBJART']
    if not subset_obj.empty:
        cases = " ".join([f"WHEN '{row['code']}' THEN '{row['klartext']}'" for _, row in subset_obj.iterrows()])
        select_parts.append(f"CASE OBJART {cases} ELSE 'Unbekannt' END AS objart_txt")
    
    # BRS Klartext
    if 'ver' in src_file.lower():
        subset_brs = lookups_df[lookups_df['kategorie'] == 'BRS']
        if not subset_brs.empty:
            cases_brs = " ".join([f"WHEN {row['code']} THEN '{row['klartext']}'" for _, row in subset_brs.iterrows()])
            select_parts.append(f"CASE BRS {cases_brs} ELSE 'Sonstige' END AS klasse_txt")

    query = f"SELECT {', '.join(select_parts)} FROM {src_file}"
    if pd.notna(filter_sql) and filter_sql.strip() != "":
        query += f" WHERE {filter_sql}"
    
    return query

def inject_style(gpkg_path, layer_name, qml_path):
    if not os.path.exists(qml_path):
        return
    with open(qml_path, 'r', encoding='utf-8') as f:
        style_xml = f.read()
    
    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS layer_styles (id INTEGER PRIMARY KEY AUTOINCREMENT, f_table_name TEXT, styleName TEXT, styleQML TEXT, useAsDefault BOOLEAN, update_time DATETIME DEFAULT CURRENT_TIMESTAMP)")
    cur.execute("DELETE FROM layer_styles WHERE f_table_name = ?", (layer_name,))
    cur.execute("INSERT INTO layer_styles (f_table_name, styleName, styleQML, useAsDefault, update_time) VALUES (?, 'default', ?, 1, ?)", 
                (layer_name, style_xml, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='DLM250 Optimizer: Konvertiert BKG Shapefiles in ein strukturiertes GeoPackage.')
    
    # CLI Parameter Definition
    parser.add_argument('-m', '--mapping', required=True, help='Pfad zur mapping.csv')
    parser.add_argument('-l', '--lookups', required=True, help='Pfad zur lookups.csv')
    parser.add_argument('-o', '--output', required=True, help='Name/Pfad des Ziel-GeoPackages (.gpkg)')
    parser.add_argument('-f', '--force', action='store_true', help='Vorhandenes GeoPackage ohne Nachfrage ueberschreiben')

    args = parser.parse_args()

    # 1. Cleanup / Check
    if os.path.exists(args.output):
        if args.force:
            os.remove(args.output)
        else:
            check = input(f"Datei {args.output} ueberschreiben? (y/n): ")
            if check.lower() == 'y':
                os.remove(args.output)
            else:
                print("Abbruch durch Nutzer.")
                return

    # 2. Tabellen laden
    mapping = pd.read_csv(args.mapping)
    lookups = pd.read_csv(args.lookups, dtype={'code': str})

    # 3. Import-Loop
    for _, row in mapping.iterrows():
        src_path = row['src_file'] + ".shp"
        if not os.path.exists(src_path):
            print(f"Warnung: {src_path} nicht gefunden.")
            continue
        
        target_name = f"{row['target_layer']}{row['suffix']}"
        sql = build_sql_query(row['src_file'], row['filter_sql'], lookups)
        
        print(f"--> Erstelle Layer: {target_name}...")
        cmd = [
            'ogr2ogr', '-f', 'GPKG', '-update', '-append', args.output, src_path,
            '-nln', target_name, '-sql', sql,
            '-lco', 'ENCODING=UTF-8', '-lco', 'SPATIAL_INDEX=YES',
            '-nlt', 'PROMOTE_TO_MULTI', '-oo', 'ENCODING=ISO-8859-1'
        ]
        
        try:
            subprocess.run(cmd, check=True)
            if pd.notna(row['style_file']):
                inject_style(args.output, target_name, row['style_file'])
        except subprocess.CalledProcessError as e:
            print(f"Fehler bei Layer {target_name}: {e}")

    # 4. Lookups als Tabelle mitspeichern
    conn = sqlite3.connect(args.output)
    lookups.to_sql('referenz_lookups', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"\nErfolg! GeoPackage erstellt: {args.output}")

if __name__ == "__main__":
    main()
