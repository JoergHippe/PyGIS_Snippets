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

Ziel: In wenigen Schritten von DLM250-Shapefiles zu einem fertigen GeoPackage kommen.

### 1. Voraussetzungen

- Python 3.10+
- GDAL/`ogr2ogr`
- Dateien:
  - `ETL_DLM250/mapping.csv`
  - `ETL_DLM250/lookups.csv`
  - DLM250-Shapefiles (`*.shp`, `*.dbf`, `*.shx`, ...)

### 2. Richtige Shell starten (Windows)

Empfohlen: **OSGeo4W Shell** oder **QGIS Shell**.

Dann testen:

```bash
python --version
ogr2ogr --version
```

Beide Befehle muessen funktionieren.

Hinweis:
- In OSGeo4W/QGIS-Shell `python` verwenden (nicht `py`).

### 3. In den Projektordner wechseln

Beispiel:

```bash
cd "d:\Coding\GitHub Repos\PyGIS_Snippets"
```

### 4. Skript einmal aufrufbar pruefen

OSGeo4W/QGIS Shell (cmd):
```bat
python ETL_DLM250\DLM250-to-GPKG.py --help
```

PowerShell:
```bash
python ETL_DLM250/DLM250-to-GPKG.py --help
```

Wenn das klappt, ist die Laufzeitumgebung korrekt.

### 5. Vollen Import starten (normaler Fall)

OSGeo4W/QGIS Shell (cmd, einzeilig):
```bat
python ETL_DLM250\DLM250-to-GPKG.py -m ETL_DLM250\mapping.csv -l ETL_DLM250\lookups.csv -o ETL_DLM250\out\Deutschland.gpkg -s "<PFAD_ZUM_DLM250_SHP_ORDNER>" -f
```

OSGeo4W/QGIS Shell (cmd, mehrzeilig mit `^`):
```bat
python ETL_DLM250\DLM250-to-GPKG.py ^
  -m ETL_DLM250\mapping.csv ^
  -l ETL_DLM250\lookups.csv ^
  -o ETL_DLM250\out\Deutschland.gpkg ^
  -s "<PFAD_ZUM_DLM250_SHP_ORDNER>" ^
  -f
```

PowerShell (mehrzeilig mit Backtick):
```powershell
python ETL_DLM250/DLM250-to-GPKG.py `
  -m ETL_DLM250/mapping.csv `
  -l ETL_DLM250/lookups.csv `
  -o ETL_DLM250/out/Deutschland.gpkg `
  -s "<PFAD_ZUM_DLM250_SHP_ORDNER>" `
  -f
```

Was passiert dabei:
- liest `mapping.csv` und `lookups.csv`
- importiert alle gefundenen Layer
- ueberspringt fehlende Quellen mit Warnung
- legt den Ausgabeordner aus `-o` bei Bedarf automatisch an
- schreibt `referenz_lookups` ins GPKG
- meldet am Ende Fehler/Warnungen gesammelt
- schreibt ein Logfile neben das GPKG (Default: gleicher Name mit `.log`)
- vorhandene Logdatei wird pro Lauf ueberschrieben
- Loginhalt: nur Warnungen und Fehler (keine Erfolgs-Infos)

### 6. Einzeldatei testen (schneller Check)

OSGeo4W/QGIS Shell (cmd):
```bat
python ETL_DLM250\DLM250-to-GPKG.py -m ETL_DLM250\mapping.csv -l ETL_DLM250\lookups.csv -o ETL_DLM250\out\Test.gpkg -i "<PFAD_ZU_EINER_SHP_DATEI>"
```

PowerShell:
```powershell
python ETL_DLM250/DLM250-to-GPKG.py `
  -m ETL_DLM250/mapping.csv `
  -l ETL_DLM250/lookups.csv `
  -o ETL_DLM250/out/Test.gpkg `
  -i "<PFAD_ZU_EINER_SHP_DATEI>"
```

### 7. Parameter kurz erklaert

| Parameter   | Kurzform | Bedeutung |
|-------------|----------|-----------|
| `--mapping` | `-m` | Steuerdatei mit Quell-/Ziellayern |
| `--lookups` | `-l` | Lookup-Tabelle fuer Klartext |
| `--output`  | `-o` | Ziel-GeoPackage (`.gpkg`) |
| `--source`  | `-s` | Quellordner fuer Batch-Import |
| `--input`   | `-i` | Einzelne Shapefile fuer Testlauf |
| `--force`   | `-f` | Ziel-GPKG ohne Rueckfrage loeschen/neu erstellen |
| `--log`     | -    | Optionaler Pfad zur Logdatei |

### 8. Typische Stolpersteine

- `ogr2ogr` nicht gefunden: falsche Shell, nicht in OSGeo4W/QGIS gestartet.
- Mehrzeilige Befehle schlagen fehl: in cmd `^` statt `\` verwenden; in PowerShell Backtick statt `\`.
- `Keine passenden Eingabedaten`: `--source` falsch oder Dateinamen passen nicht zu `src_file` in `mapping.csv`.
- Nur Teilimport: einige Quellen fehlen im `--source`-Ordner (wird als Warnliste ausgegeben).

## Entwicklung

### Architektur und Ablauf

1. Einlesen und Validierung von `mapping.csv` und `lookups.csv`.
2. Ermittlung der zu verarbeitenden Quellen (Batch oder Einzeldatei).
3. Aufbau eines SQL-Statements pro Quelllayer (`SELECT *` plus `objart_txt` via `CASE OBJART`).
4. Import mit `ogr2ogr` in das Ziel-GPKG.

- Bei Append wird `-addfields` genutzt, damit abweichende Felder in Sammellayern nicht zum Abbruch fuehren.

1. Optionales Schreiben von Styles in `layer_styles` (wenn `style_file` gesetzt und Datei vorhanden).
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

Falsches Python wird verwendet:

- Wenn mehrere Python-Versionen installiert sind, mit `py -3` starten.
- In VS Code den Interpreter der gewuenschten Umgebung waehlen.

Umlaute/Encoding-Probleme:

- Quell-Shapefiles sind oft `ISO-8859-1`. Das Skript nutzt `-oo ENCODING=ISO-8859-1`.

Keine Styles sichtbar:

- Aktuell sind Styles optional. Ohne physische `.qml`-Dateien wird nur eine Zusammenfassung ausgegeben.

## Status

Aktiv in Nutzung fuer DLM250-ETL (Stufe 1).
