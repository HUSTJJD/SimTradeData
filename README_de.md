[Englisch](README.md) | [Chinesisch](README_zh.md) | Deutsch

# SimTradeData

Lokale Backtesting-Daten für [SimTradeLab](https://github.com/kay-ou/SimTradeLab) und SimTradeDesk: chinesische Aktien, ETFs und US-Aktien. Speicherung in DuckDB, Export als gemeinsame Parquet-Datenpakete.

## Weboberfläche starten

Python 3.10+, [uv](https://docs.astral.sh/uv/getting-started/installation/) und Node.js 20+ mit npm installieren. Im Projektverzeichnis:

```bash
npm run ui
```

Die im Terminal angezeigte Adresse öffnen (ab **http://127.0.0.1:8765**, belegte Ports werden automatisch übersprungen). Optional einen festen Port wählen:

```bash
npm run ui -- --port 8767
```

Mit **Backtesting-Daten aktualisieren** werden Aktien und ETFs gemeinsam ergänzt. Gültige vorhandene Daten werden übersprungen. Fortschritt, Abbruch und Wiederholung fehlgeschlagener Einheiten sind sichtbar. Aktien- und ETF-Statistiken werden getrennt angezeigt; Quartalsberichte und ETF-PCF stehen unter **Weitere Daten**.

Unter **Backtestdaten exportieren** CN- oder US-Datenpakete erstellen und herunterladen. In das Datenverzeichnis von SimTradeLab / SimTradeDesk entpacken und die Verzeichnisstruktur beibehalten. Der US-Download erfolgt weiterhin über die CLI.

Standarddatenbank: `data/cn.duckdb`; anderes Verzeichnis mit `--data-dir /path/to/data`. Der Datenbestand bestimmt den Aktualisierungsbedarf; kein gespeicherter Downloadverlauf. `npm run ui` installiert Abhängigkeiten und baut bei Bedarf automatisch. Der erste freie Port ab 8765 wird gewählt; die Adresse steht im Terminal. Chinesisch, Englisch und Deutsch sind oben umschaltbar; die erste Auswahl folgt dem Browser; die manuelle Auswahl wird gespeichert.

## Fertige Daten oder CLI

[Releases](https://github.com/kay-ou/SimTradeData/releases) bieten `data-cn-v*` und `data-us-v*`. Parquet-Dateien nach SimTradeLab `data/cn/` bzw. `data/us/` entpacken. Diese Pakete sind keine aktualisierbaren DuckDB-Quelldatenbanken.

Für die CLI Python und uv installieren, `uv sync` ausführen, dann:

```bash
uv run python scripts/download.py
uv run python scripts/export_parquet.py
```

Standardexport: `data/export/cn/`. Weboberfläche und CLI nicht gleichzeitig in dieselbe Datenbank schreiben lassen.

## Vollständige Historie und Datenpflege

Seit 2026-11 reicht der kanonische CN-Tageskursbestand bis zum **19.12.1990**
zurück (Beginn des A-Aktienmarkts); Handelskalender, Ex-Rechte und das Manifest
des Datenpakets verwenden denselben Startwert. Die vollständige Rekonstruktion
läuft über `unified_data_manager.py --apply --full-history --history-start
1990-12-19`; der normale inkrementelle Lauf bleibt bei 24 Monaten und ergänzt
keine 1990er Jahre.

**Grenzen der 1990er Jahre**: Die offiziellen PTrade-Kurs-APIs
(`get_history` / `get_price`) liefern nur Daten ab 2005; `get_trade_days`
erlaubt einen Kalenderstart ab 1990. Der lokale 1990er-Bestand ist eine
Erweiterung des Offline-Pakets, keine offizielle Backtest-Fähigkeit.
Finanzdaten aus den 1990ern sind nur sehr dünn vorhanden, ETFs und
Indexzusammensetzungen existierten damals noch nicht. Zusätzliche
Index-Historie-Assets oder Longhubang-Daten werden nicht unterstützt:
PTrade bietet dafür keine Daten-API und SimTradeLab keinen Lesepfad.

`unified_data_manager.py` prüft vor der Veröffentlichung die Datenqualität,
gleicht optional lokale QMT-DAT-Dateien ab (0,5 %-Toleranz; systematische
Konflikte werden protokolliert und übersprungen), exportiert stufenweise und
führt danach die strenge Integritätsprüfung sowie einen PTrade-Speicher-
Smoke-Test aus. Ohne `--apply` ist alles ein schreibgeschützter Probelauf.
QMT ist optional und begrenzt niemals das Zieldatum; Internetquellen haben
Vorrang.

`data_health_report.py` erzeugt einen schreibgeschützten Bericht über
Abdeckung, Zeiträume und auffällige Zeilen. Ein `pass` der Veröffentlichungs-
schranke bedeutet nicht, dass alle Warnungen behoben sind: Warnungen erfordern
eine Quellaktualisierung oder Quarantäne, niemals erfundene Werte.

## Dokumentation und Grenzen

| Thema | Anleitung |
| --- | --- |
| Oberfläche und Datenpflege | [Web-Anleitung](docs/WEB_DATA_ADMIN.md) |
| US-Daten und Parquet-Export | [CLI-Anleitung](docs/CLI_GUIDE.md) |
| Datenabdeckung und PTrade-Grenzen | [Datenabdeckung](docs/PTRADE_DATA_COVERAGE.md) |
| CI, Überwachung und Wiederherstellung | [Betrieb](docs/OPERATIONS.md) |
| Formate, Finanzfelder und PCF | [Dokumentationsübersicht](docs/README.md) |

Detailanleitungen werden auf Chinesisch gepflegt; die Betriebsdokumentation enthält Englisch. 223 öffentliche Finanzfelder sind zugeordnet. Vollständige PTrade-Gleichwertigkeit, historische Wertpapierlisten und lückenlose PCF-Historien sind nicht garantiert. ETF-Kursbacktests benötigen keine PCF. Erfolgreiche Stichproben beweisen keine vollständige Historie.

[Änderungen](CHANGELOG.md) · [Lizenz: AGPL-3.0](LICENSE)
