import argparse
import csv
import logging
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import datetime
from typing import TypedDict


class LookupRow(TypedDict):
    kategorie: str
    code: str
    klartext: str


class MappingRow(TypedDict):
    src_file: str
    target_layer: str
    suffix: str
    filter_sql: str
    style_file: str


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _is_non_empty(value: str) -> bool:
    return value.strip() != ""


def _required_columns_exist(columns: list[str], required: set[str]) -> tuple[bool, list[str]]:
    existing = {column.strip() for column in columns}
    missing = sorted(required - existing)
    return len(missing) == 0, missing


def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("dlm250")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_handler.addFilter(lambda record: record.levelno != logging.WARNING)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def collect_ogr_messages(
    text: str, warning_counter: Counter[str], error_messages: list[str]
) -> None:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lower = line.lower()
        if lower.startswith("warning"):
            warning_counter[line] += 1
        elif lower.startswith("error"):
            error_messages.append(line)


def _read_lookups_csv(path: str) -> list[LookupRow]:
    required = {"kategorie", "code", "klartext"}
    with open(path, "r", encoding="utf-8", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        fieldnames = list(reader.fieldnames or [])
        ok, missing = _required_columns_exist(fieldnames, required)
        if not ok:
            print(f"Fehler: lookups.csv fehlt Spalte(n): {', '.join(missing)}")
            sys.exit(1)

        rows: list[LookupRow] = []
        for raw_row in reader:
            kategorie = str(raw_row.get("kategorie", "")).strip()
            code = str(raw_row.get("code", "")).strip()
            klartext = str(raw_row.get("klartext", "")).strip()
            if not _is_non_empty(kategorie) or not _is_non_empty(code):
                continue
            rows.append({"kategorie": kategorie, "code": code, "klartext": klartext})

    return rows


def _read_mapping_csv(path: str) -> list[MappingRow]:
    required = {"src_file", "target_layer", "suffix", "filter_sql", "style_file"}
    with open(path, "r", encoding="utf-8", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        fieldnames = list(reader.fieldnames or [])
        ok, missing = _required_columns_exist(fieldnames, required)
        if not ok:
            print(f"Fehler: mapping.csv fehlt Spalte(n): {', '.join(missing)}")
            sys.exit(1)

        rows: list[MappingRow] = []
        for raw_row in reader:
            src_file = str(raw_row.get("src_file", "")).strip()
            if not _is_non_empty(src_file) or src_file.startswith("#"):
                continue

            target_layer = str(raw_row.get("target_layer", "")).strip()
            suffix = str(raw_row.get("suffix", "")).strip()
            filter_sql = str(raw_row.get("filter_sql", "")).strip()
            style_file = str(raw_row.get("style_file", "")).strip()

            if not _is_non_empty(target_layer):
                continue

            rows.append(
                {
                    "src_file": src_file,
                    "target_layer": target_layer,
                    "suffix": suffix,
                    "filter_sql": filter_sql,
                    "style_file": style_file,
                }
            )

    return rows


def build_sql_query(src_file_name: str, filter_sql: str, lookups: list[LookupRow]) -> str:
    """Baut das SQL mit Klartext-Transformationen."""
    select_parts = ["*"]

    objart_entries = [row for row in lookups if row["kategorie"] == "OBJART"]
    if objart_entries:
        objart_cases = " ".join(
            [
                f"WHEN '{_escape_sql_literal(row['code'])}' THEN '{_escape_sql_literal(row['klartext'])}'"
                for row in objart_entries
            ]
        )
        select_parts.append(f"CASE OBJART {objart_cases} ELSE 'Unbekannt' END AS objart_klartext")

    query = f"SELECT {', '.join(select_parts)} FROM \"{src_file_name}\""
    if _is_non_empty(filter_sql):
        query += f" WHERE {filter_sql}"

    return query


def inject_style(gpkg_path: str, layer_name: str, qml_path: str) -> bool:
    if not os.path.exists(qml_path):
        return False

    with open(qml_path, "r", encoding="utf-8") as file_handle:
        style_xml = file_handle.read()

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS layer_styles ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "f_table_name TEXT, "
        "styleName TEXT, "
        "styleQML TEXT, "
        "useAsDefault BOOLEAN, "
        "update_time DATETIME DEFAULT CURRENT_TIMESTAMP)"
    )
    cur.execute("DELETE FROM layer_styles WHERE f_table_name = ?", (layer_name,))
    cur.execute(
        "INSERT INTO layer_styles (f_table_name, styleName, styleQML, useAsDefault, update_time) "
        "VALUES (?, 'default', ?, 1, ?)",
        (layer_name, style_xml, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    return True


def write_lookups_table(gpkg_path: str, lookups: list[LookupRow]) -> None:
    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS referenz_lookups")
    cur.execute(
        "CREATE TABLE referenz_lookups ("
        "kategorie TEXT NOT NULL, "
        "code TEXT NOT NULL, "
        "klartext TEXT)"
    )
    cur.executemany(
        "INSERT INTO referenz_lookups (kategorie, code, klartext) VALUES (?, ?, ?)",
        [(row["kategorie"], row["code"], row["klartext"]) for row in lookups],
    )
    conn.commit()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DLM250 Optimizer: Konvertiert BKG Shapefiles in ein strukturiertes GeoPackage."
    )

    parser.add_argument("-m", "--mapping", required=True, help="Pfad zur mapping.csv")
    parser.add_argument("-l", "--lookups", required=True, help="Pfad zur lookups.csv")
    parser.add_argument("-o", "--output", required=True, help="Ziel-GeoPackage (.gpkg)")
    parser.add_argument("-s", "--source", help="Quell-ORDNER (verarbeitet alle Schichten laut Mapping)")
    parser.add_argument("-i", "--input", help="Einzelne Quell-DATEI (.shp)")
    parser.add_argument("-f", "--force", action="store_true", help="Vorhandenes GPKG ohne Nachfrage ueberschreiben")
    parser.add_argument("--log", help="Optionaler Pfad zur Logdatei")

    args = parser.parse_args()

    if not args.source and not args.input:
        print("Fehler: Bitte entweder --source (Ordner) oder --input (Datei) angeben.")
        sys.exit(1)

    output_dir = os.path.dirname(os.path.abspath(args.output))
    if _is_non_empty(output_dir) and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except OSError as error:
            print(f"Fehler: Ausgabeverzeichnis kann nicht erstellt werden: {output_dir} ({error})")
            sys.exit(1)

    log_path = args.log or f"{os.path.splitext(args.output)[0]}.log"
    logger = setup_logger(log_path)

    if os.path.exists(args.output):
        if args.force or input(f"Datei {args.output} ueberschreiben? (y/n): ").lower() == "y":
            os.remove(args.output)
        else:
            logger.warning("Abbruch.")
            return

    mapping_rows = _read_mapping_csv(args.mapping)
    lookup_rows = _read_lookups_csv(args.lookups)
    mapping_dir = os.path.dirname(os.path.abspath(args.mapping))

    to_process: list[tuple[str, MappingRow]] = []
    missing_sources: list[str] = []
    if args.input:
        if not os.path.exists(args.input):
            logger.error(f"Fehler: Eingabedatei nicht gefunden: {args.input}")
            sys.exit(1)
        base_name = os.path.splitext(os.path.basename(args.input))[0]
        matches = [row for row in mapping_rows if row["src_file"] == base_name]
        for row in matches:
            to_process.append((args.input, row))
    else:
        if args.source is None:
            logger.error("Fehler: --source fehlt.")
            sys.exit(1)
        for row in mapping_rows:
            full_path = os.path.join(args.source, row["src_file"] + ".shp")
            if os.path.exists(full_path):
                to_process.append((full_path, row))
            else:
                missing_sources.append(full_path)

    if missing_sources:
        logger.warning("Warnung: Fehlende Quell-Shapefiles (werden uebersprungen):")
        for path in missing_sources:
            logger.warning(f"- {path}")

    if not to_process:
        logger.error("Fehler: Keine passenden Eingabedaten laut Mapping gefunden.")
        sys.exit(1)

    output_exists = os.path.exists(args.output)
    missing_styles: list[str] = []
    conversion_errors: list[str] = []
    warning_counter: Counter[str] = Counter()
    ogr_error_lines: list[str] = []
    processed_layers = 0
    for full_path, row in to_process:
        base_name = row["src_file"]
        target_name = f"{row['target_layer']}{row['suffix']}"
        sql = build_sql_query(base_name, row["filter_sql"], lookup_rows)

        logger.info(f"--> Erstelle: {target_name} aus {os.path.basename(full_path)}")
        cmd: list[str] = ["ogr2ogr", "-f", "GPKG"]
        if output_exists:
            cmd.extend(["-update", "-append", "-addfields"])

        cmd.extend(
            [
                args.output,
                full_path,
                "-nln",
                target_name,
                "-dialect",
                "SQLite",
                "-sql",
                sql,
                "-lco",
                "SPATIAL_INDEX=YES",
                "-nlt",
                "PROMOTE_TO_MULTI",
                "-oo",
                "ENCODING=ISO-8859-1",
            ]
        )

        try:
            process = subprocess.run(cmd, check=True, capture_output=True, text=True)
            if process.stderr:
                collect_ogr_messages(process.stderr, warning_counter, ogr_error_lines)
            output_exists = True
            processed_layers += 1
            if _is_non_empty(row["style_file"]):
                style_path = row["style_file"]
                if not os.path.isabs(style_path):
                    style_path = os.path.join(mapping_dir, style_path)
                style_ok = inject_style(args.output, target_name, style_path)
                if not style_ok:
                    missing_styles.append(f"{target_name}: {style_path}")
        except subprocess.CalledProcessError as error:
            if error.stderr:
                collect_ogr_messages(str(error.stderr), warning_counter, ogr_error_lines)
            message = f"{target_name}: {error}"
            conversion_errors.append(message)
            logger.error(f"Fehler bei {message}")
        except FileNotFoundError:
            message = "ogr2ogr wurde nicht gefunden (GDAL nicht im PATH)."
            conversion_errors.append(message)
            logger.error(f"Fehler: {message}")
            break

    if output_exists:
        write_lookups_table(args.output, lookup_rows)

    if warning_counter:
        total_warnings = sum(warning_counter.values())
        logger.warning("Warnungszusammenfassung (aggregiert):")
        for warning, count in warning_counter.most_common():
            logger.warning(f"{count}x {warning}")
        logger.info(f"Hinweis: {total_warnings} Warnungen erkannt (Details in Logdatei).")

    if ogr_error_lines:
        logger.error("OGR-Fehlerdetails:")
        for line in ogr_error_lines:
            logger.error(f"- {line}")

    if conversion_errors:
        logger.error("Import-Fehlerzusammenfassung:")
        for entry in conversion_errors:
            logger.error(f"- {entry}")
    if missing_styles:
        logger.warning("Style-Fehlerzusammenfassung (kein Abbruch):")
        for entry in missing_styles:
            logger.warning(f"- Fehlende Style-Datei: {entry}")
    if conversion_errors:
        logger.error(f"Abgeschlossen mit Fehlern. GPKG: {args.output}. Log: {log_path}")
        sys.exit(1)
    if not output_exists:
        logger.error(f"Abgeschlossen ohne Datenimport. Keine GPKG-Datei erstellt: {args.output}. Log: {log_path}")
        sys.exit(1)
    logger.info(
        f"Fertig: {processed_layers}/{len(to_process)} Layer verarbeitet. "
        f"Status: fehlerfrei. Log: {log_path}"
    )


if __name__ == "__main__":
    main()
