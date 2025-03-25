# Importieren der benötigten Bibliotheken
import openai                    # Für den Zugriff auf OpenAI's GPT-Modelle
import pandas as pd             # Für Datenmanipulation und Analyse mit DataFrames
from sqlalchemy import create_engine  # Für die Verbindung zu einer SQL-Datenbank

# Datenbank-Verbindungsdetails
DATABASE = "longeval"           # Name der Datenbank
USER = "dis18"                  # Benutzername
HOST = "db"                     # Hostname des Datenbankservers
PORT = "5432"                   # Port für PostgreSQL
PASSWORD = "dis182425"         # Passwort für die Datenbank

# Aufbau der Verbindung zur PostgreSQL-Datenbank mit SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# SQL-Abfrage: Auswahl von bestimmten Spalten aus der Tabelle "Topic"
query = '''
SELECT 
    "queryid" AS ID,              "
    "text_en" AS Query,           "
    "sub_collection" AS Zeitpunkt "
FROM "Topic"
WHERE "sub_collection" = '2023-01';  -- Auswahl nur der Einträge aus Januar 2023
'''

# Ausführen der SQL-Abfrage und Laden der Ergebnisse in ein DataFrame
df = pd.read_sql(query, con=engine)

# Setzen des OpenAI API-Schlüssels
openai.api_key = "KEY"

# Funktion zur Bewertung, wie zeitabhängig eine Query ist
def get_infos(query):
    # Prompt, der an das LLM gesendet wird
    prompt = f"You have this Query. Give a score on how time dependant this Query:{query}. The Score is between 0 to 1. Don't answer with anything more than the Score."

    # Aufruf des GPT-Modells zur Bewertung der Query
    response = openai.chat.completions.create(
        model="gpt-4o-mini",  # Modellwahl: leichtgewichtiges GPT-4-Modell
        messages=[{"role": "user", "content": prompt}],  # Nutzer-Message mit dem Prompt
        max_tokens=50,        # Begrenzung der Ausgabe auf max. 50 Tokens
        temperature=0         # Temperature = 0 für deterministische, konsistente Antworten
    )

    # Extrahieren der Modell-Antwort (nur der Score wird erwartet)
    tag = response.choices[0].message.content.strip()

    # Ausgabe zur Nachverfolgung im Terminal
    print(f"Processed Query: {query} -> Tag: {tag}")

    # Rückgabe des Scores
    return tag

# Anwenden der get_infos-Funktion auf jede Query im DataFrame
df['tag'] = df['Query'].apply(get_infos)

# Speichern des erweiterten DataFrames als CSV-Datei
output_file = "Expanded/Query_Expansion_231.csv"
df.to_csv(output_file, index=False)
