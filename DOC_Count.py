import pandas as pd # Importieren von Pandas zur Datenverarbeitung
from sqlalchemy import create_engine # Importieren der create_engine-Funktion zur Herstellung einer Datenbankverbindung

# Datenbankverbindungsdetails
DATABASE = "longeval" # Name der Datenbank
USER = "dis18" # Benutzername für die Datenbank
HOST = "db" # Hostname des Datenbankservers
PORT = "5432" # Standard-Port für PostgreSQL
PASSWORD = "dis182425" # Passwort für die Datenbankverbindung

# Erstellen einer Verbindung zur PostgreSQL-Datenbank
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# SQL-Abfrage: Auswahl von relevanten Dokumenten mit Metadaten
query = '''
SELECT
    distinct "Topic"."queryid" AS ID,
    "Topic"."text_en" AS Query,
    "Qrel"."docid" AS DocID,
    "Qrel"."relevance" AS Relevanz,
    "Document"."sub_collection" AS Datum,
    "Document"."url"
FROM "Topic"
JOIN "Qrel" ON "Topic"."queryid" = "Qrel"."queryid"
JOIN "Document" ON "Qrel"."docid" = "Document"."docid"
WHERE "Qrel"."relevance" > 0;
'''

# SQL-Abfrage: Berechnen, wie oft jede URL in der Datenbank vorkommt
query2 = """
SELECT 
    "url", 
    "docid",
    COUNT(*) OVER(PARTITION BY "url") AS Anzahl_Vorkommen
FROM "Document"
ORDER BY Anzahl_Vorkommen DESC;

"""

# Ausführen der ersten SQL-Abfrage und Speichern der Ergebnisse in einem Pandas-DataFrame
df = pd.read_sql(query, con=engine)

# Ausführen der zweiten SQL-Abfrage und Speichern der Ergebnisse in einem weiteren DataFrame
df2 =  pd.read_sql(query2, con=engine)


# Erster Merge: Verknüpfen von df (relevante Dokumente) mit df2 (Anzahl der Vorkommen) anhand der Spalte 'url'
merged_df = pd.merge(df, df2, left_on='url', right_on='url', how='inner')

# Umbenennen der Spalte 'docid_x' zu 'docid', falls das ein einheitliches Format erfordert
merged_df = merged_df.rename(columns={'docid_x': 'docid'})
print(merged_df.columns)

# Einlesen einer CSV-Datei mit BM25-Ergebnissen
csv_df = pd.read_csv("bm25_results.csv")

# Sicherstellen, dass die Spalten 'docid' (aus merged_df) und 'docno' (aus csv_df) als Strings behandelt werden,
# um Probleme beim Merge zu vermeiden
merged_df['docid'] = merged_df['docid'].astype(str)
csv_df['docno'] = csv_df['docno'].astype(str)

# Zweiter Merge: Verknüpfen der vorher zusammengeführten Daten mit der CSV-Datei anhand der Spalten 'docid' und 'docno'
merged_df_final = pd.merge(merged_df, csv_df, left_on='docid', right_on='docno', how='inner')


# Anzeigen des finalen DataFrame mit den vollständigen Daten
print(merged_df_final)

# Speichern des finalen DataFrame als CSV-Datei im Verzeichnis 'Expanded'
output_file = "Expanded/Vorkommen_Join2.csv"
merged_df_final.to_csv(output_file, index=False)
