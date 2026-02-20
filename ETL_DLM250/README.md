# DLM250 Konvertierungs-Workflow (Stufe 1)

## 1. Komponenten

- `mapping.csv`: Steuert, welche Shapefiles in welche GPKG-Layer fliessen (Suffix `_F`, `_L`, `_P`).
- `lookups.csv`: Zentrale Tabelle fuer die Uebersetzung von Codes (z. B. `OBJART`, `WDM`) in Klartext.
- `DLM250-to-GPKG.py`: Nutzt `ogr2ogr` fuer den Import und `sqlite3` fuer Style-Injektion und Lookup-Speicherung.
- Referenzdaten: `docs/dlm250_ebenen_inhalt.txt` (fachliche Fuehrungsquelle) und `docs/dlm250.pdf`.

## 2. Kernfunktionen des Skripts

1. Bereinigung: Loescht altes GPKG nach Bestaetigung.
2. Klartext-Injection: Baut waehrend des Imports `CASE`-Statements in das SQL ein.
3. Lookup-Export: Schreibt die `lookups.csv` als Datentabelle in das GPKG.
4. Styling: Liest `.qml`-Dateien aus und speichert sie in `layer_styles`.

Hinweis: Beim AGS immer den Datentyp String beibehalten.
