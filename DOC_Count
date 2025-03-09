import pandas as pd
from sqlalchemy import create_engine

DATABASE = "longeval"
USER = "dis18"
HOST = "db"
PORT = "5432"
PASSWORD = "dis182425"

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

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

query2 = """
SELECT 
    "url", 
    "docid",
    COUNT(*) OVER(PARTITION BY "url") AS Anzahl_Vorkommen
FROM "Document"
ORDER BY Anzahl_Vorkommen DESC;

"""

df = pd.read_sql(query, con=engine)

df2 =  pd.read_sql(query2, con=engine)

# Erster Merge: df mit df2
merged_df = pd.merge(df, df2, left_on='url', right_on='url', how='inner')
# Optional: Umbenennen der Spalte 'DocID' (aus df) in 'docid', falls du beide in einem gemeinsamen Format haben möchtest
merged_df = merged_df.rename(columns={'docid_x': 'docid'})
print(merged_df.columns)
# Lese die CSV-Datei ein
csv_df = pd.read_csv("bm25_results.csv")

# Stelle sicher, dass die Spalten den gleichen Typ haben
merged_df['docid'] = merged_df['docid'].astype(str)
csv_df['docno'] = csv_df['docno'].astype(str)

# Zweiter Merge: mit csv_df
merged_df_final = pd.merge(merged_df, csv_df, left_on='docid', right_on='docno', how='inner')
print(merged_df_final)
output_file = "Expanded/Vorkommen_Join2.csv"
merged_df_final.to_csv(output_file, index=False)
