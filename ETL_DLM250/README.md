# DLM250-to-GPKG Optimizer

Ein CLI-Tool zur Transformation des Digitalen Landschaftsmodells 1:250.000 (DLM250) vom Bundesamt fuer Kartographie und Geodaesie (BKG) in ein strukturiertes GeoPackage.

## Einleitung

DLM250 wird als Sammlung vieler Shapefiles mit technischen Namen (z. B. `geb01_f.shp`) und fachlichen Codewerten ausgeliefert.
Dieses Projekt standardisiert den ETL-Prozess in ein einzelnes `.gpkg`, damit die Daten direkt in QGIS weiterverarbeitet werden koennen.

Ziele:

- Strukturierung: Zusammenfuehrung vieler Shapefiles in eine GeoPackage-Datei.
- Semantische Klarheit: Ziel-Layernamen ueber `mapping.csv` steuerbar.
- Klartext-Anreicherung: `OBJART`-Codes werden beim Import in `objart_txt` uebersetzt.
- Portabilitaet: Lookup-Tabelle wird als `referenz_lookups` ins GPKG geschrieben.
- Robustheit: Fehlende Quellen/Styles werden zusammengefasst gemeldet.

Fachliche Referenz (Fuehrungsquelle):

- `docs/dlm250_ebenen_inhalt.txt`
- `docs/dlm250.pdf`

## Anwendungen

Typische Einsatzfaelle:

- Aufbau einer konsolidierten DLM250-Datenbasis fuer QGIS-Projekte.
- Wiederholbarer Batch-Import neuer Datenstaende.
- Vorbereitung fuer kategoriesierte Symbolisierung ueber Attribute (statt hartem Layer-Splitting pro Typ).

### Voraussetzungen

- Python 3.10+
- GDAL/`ogr2ogr` im Systempfad
- Keine externen Python-Abhaengigkeiten noetig (Standardbibliothek)

### Konfigurationsdateien

1. `mapping.csv`

- Steuert Quelllayer (`src_file`), Ziellayer (`target_layer`), Geometrietyp (`suffix`) und optionalen SQL-Filter (`filter_sql`).
- `suffix` bleibt der Geometrietyp-Indikator (`_F`, `_L`, `_P`).

1. `lookups.csv`

- Enthaelt Code-zu-Klartext-Zuordnungen, insbesondere fuer `OBJART`.
- Wird als `referenz_lookups` in das Ziel-GPKG uebernommen.

### CLI-Nutzung

Skript:

- `ETL_DLM250/DLM250-to-GPKG.py`

Gesamten Quellordner verarbeiten:

```bash
python ETL_DLM250/DLM250-to-GPKG.py \
  -m ETL_DLM250/mapping.csv \
  -l ETL_DLM250/lookups.csv \
  -o out/Deutschland.gpkg \
  -s <pfad-zum-shp-ordner> \
  -f
```

Einzelnes Shapefile verarbeiten:

```bash
python ETL_DLM250/DLM250-to-GPKG.py \
  -m ETL_DLM250/mapping.csv \
  -l ETL_DLM250/lookups.csv \
  -o out/Test.gpkg \
  -i <pfad-zu/geb01_f.shp>
```

Parameter:

| Parameter   | Kurzform | Beschreibung |
|-------------|----------|--------------|
| `--mapping` | `-m` | Pfad zur Mapping-CSV |
| `--lookups` | `-l` | Pfad zur Lookup-CSV |
| `--output`  | `-o` | Ziel-GeoPackage |
| `--source`  | `-s` | Quellordner mit Shapefiles |
| `--input`   | `-i` | Einzelnes Shapefile |
| `--force`   | `-f` | Vorhandenes GPKG ohne Rueckfrage ueberschreiben |

## Entwicklung

### Architektur und Ablauf

1. Einlesen und Validierung von `mapping.csv` und `lookups.csv`.
2. Ermittlung der zu verarbeitenden Quellen (Batch oder Einzeldatei).
3. Aufbau eines SQL-Statements pro Quelllayer (`SELECT *` plus `objart_txt` via `CASE OBJART`).
4. Import mit `ogr2ogr` in das Ziel-GPKG.

- Bei Append wird `-addfields` genutzt, damit abweichende Felder in Sammellayern nicht zum Abbruch fuehren.

5. Optionales Schreiben von Styles in `layer_styles` (wenn `style_file` gesetzt und Datei vorhanden).
2. Schreiben der Lookup-Referenztabelle `referenz_lookups`.

### Best Practices

- AGS/Gemeindeschluessel immer als String behandeln.
- `_F`, `_L`, `_P` in `suffix` konsistent pflegen.
- Kategoriesierte Symbolisierung in QGIS ueber Attribute (z. B. `OBJART`, `objart_txt`) bevorzugen.
- Konfigurationslogik ausschliesslich ueber `mapping.csv`/`lookups.csv` steuern, nicht hart im Code.

### Fehlerverhalten

- Fehlende Shapefiles im Batch werden als Warnliste ausgegeben und uebersprungen.
- Fehlende Style-Dateien werden gesammelt gemeldet, ohne Laufabbruch.
- Importfehler (inkl. fehlendem `ogr2ogr`) werden als Fehlerzusammenfassung ausgegeben; Exit-Code ist ungleich 0.

## Troubleshooting

`ogr2ogr` wird nicht gefunden:

- GDAL nicht im PATH. Unter Windows am einfachsten in der OSGeo4W-Shell bzw. QGIS-Shell ausfuehren.

Umlaute/Encoding-Probleme:

- Quell-Shapefiles sind oft `ISO-8859-1`. Das Skript nutzt `-oo ENCODING=ISO-8859-1`.

Keine Styles sichtbar:

- Aktuell sind Styles optional. Ohne physische `.qml`-Dateien wird nur eine Zusammenfassung ausgegeben.

## Status

Aktiv in Nutzung fuer DLM250-ETL (Stufe 1).
