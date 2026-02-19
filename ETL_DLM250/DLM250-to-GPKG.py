import os
import subprocess
import sqlite3
import pandas as pd
import argparse
from datetime import datetime
import sys

def build_sql_query(src_file_name, filter_sql, lookups_df):
    """Baut das SQL mit Klartext-Transformationen."""
    select_parts = ["*"]
    
    # OBJART Klartext
    subset_obj = lookups_df[lookups_df['kategorie'] == 'OBJART']
    if not subset_obj.empty:
        cases = " ".join([f"WHEN '{row['code']}' THEN '{row['klartext']}'" for _, row in subset_obj.iterrows()])
        select_parts.append(f"CASE OBJART {cases} ELSE 'Unbekannt' END AS objart_txt")
    
    # BRS Klartext (für Verkehr)
    if 'ver' in src_file_name.lower():
        subset_brs = lookups_df[lookups_df['kategorie'] == 'BRS']
        if not subset_brs.empty:
            cases_brs = " ".join([f"WHEN {row['code']} THEN '{row['klartext']}'" for _, row in subset_brs.iterrows()])
            select_parts.append(f"CASE BRS {cases_brs} ELSE 'Sonstige' END AS klasse_txt")

    query = f"SELECT {', '.join(select_parts)} FROM \"{src_file_name}\""
    if pd.notna(filter_sql) and filter_sql.strip() != "":
        query += f" WHERE {filter_sql}"
    
    return query

def inject_style(gpkg_path, layer_name, qml_path):
    if not os.path.exists(qml_path): return
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
    
    parser.add_argument('-m', '--mapping', required=True, help='Pfad zur mapping.csv')
    parser.add_argument('-l', '--lookups', required=True, help='Pfad zur lookups.csv')
    parser.add_argument('-o', '--output', required=True, help='Ziel-GeoPackage (.gpkg)')
    parser.add_argument('-s', '--source', help='Quell-ORDNER (verarbeitet alle Schichten laut Mapping)')
    parser.add_argument('-i', '--input', help='Einzelne Quell-DATEI (.shp)')
    parser.add_argument('-f', '--force', action='store_true', help='Vorhandenes GPKG ohne Nachfrage ueberschreiben')

    args = parser.parse_args()

    if not args.source and not args.input:
        print("Fehler: Bitte entweder --source (Ordner) oder --input (Datei) angeben.")
        sys.exit(1)

    # 1. GPKG Cleanup
    if os.path.exists(args.output):
        if args.force or input(f"Datei {args.output} ueberschreiben? (y/n): ").lower() == 'y':
            os.remove(args.output)
        else:
            print("Abbruch."); return

    # 2. Tabellen laden
    mapping = pd.read_csv(args.mapping)
    lookups = pd.read_csv(args.lookups, dtype={'code': str})

    # 3. Bestimmen, was verarbeitet wird
    to_process = []
    if args.input:
        # Nur eine spezifische Datei
        base_name = os.path.splitext(os.path.basename(args.input))[0]
        # Suche passende Zeilen im Mapping
        matches = mapping[mapping['src_file'] == base_name]
        for _, m_row in matches.iterrows():
            to_process.append((args.input, m_row))
    else:
        # Ganzer Ordner laut Mapping
        for _, m_row in mapping.iterrows():
            full_path = os.path.join(args.source, m_row['src_file'] + ".shp")
            if os.path.exists(full_path):
                to_process.append((full_path, m_row))

    # 4. Processing
    for full_path, m_row in to_process:
        base_name = m_row['src_file']
        target_name = f"{m_row['target_layer']}{m_row['suffix']}"
        sql = build_sql_query(base_name, m_row['filter_sql'], lookups)
        
        print(f"--> Erstelle: {target_name} aus {os.path.basename(full_path)}")
        
        cmd = [
            'ogr2ogr', '-f', 'GPKG', '-update', '-append', args.output, full_path,
            '-nln', target_name, '-sql', sql,
            '-lco', 'ENCODING=UTF-8', '-lco', 'SPATIAL_INDEX=YES',
            '-nlt', 'PROMOTE_TO_MULTI', '-oo', 'ENCODING=ISO-8859-1'
        ]
        
        try:
            subprocess.run(cmd, check=True)
            if pd.notna(m_row['style_file']):
                inject_style(args.output, target_name, m_row['style_file'])
        except subprocess.CalledProcessError as e:
            print(f"Fehler bei {target_name}: {e}")

    # 5. Lookups mitspeichern
    conn = sqlite3.connect(args.output)
    lookups.to_sql('referenz_lookups', conn, if_exists='replace', index=False)
    conn.close()
    print(f"\nFertig! GPKG: {args.output}")

if __name__ == "__main__":
    main()
