# DLM250 Konvertierungs-Workflow (Stufe 1)

## 1. Komponenten

* `mapping.csv`: Steuert, welche Shapefiles in welche GPKG-Layer fließen (mit Suffix `_F`, `_L`, `_P`).

* `lookups.csv`: Zentrale Tabelle für die Übersetzung von Codes (OBJART, BRS) in Klartext.

* Python-Skript: Nutzt `ogr2ogr` für den schnellen Import und `sqlite3` für die Style-Injektion und Metadaten.

## 2. Kernfunktionen des Skripts

1. Bereinigung: Löscht altes GPKG nach Bestätigung.

2. Klartext-Injection: Baut während des Imports `CASE`-Statements in das SQL ein.

3. Lookup-Export: Schreibt die `lookups.csv` als reine Datentabelle (ohne Geometrie) in das GPKG.

4. Styling: Liest `.qml` Dateien aus und speichert sie in der Tabelle `layer_styles`.

5. Beispiel für `lookups.csv`

---

Hinweis: Beim AGS immer den Datentyp String beibehalten!
