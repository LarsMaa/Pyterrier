import openai
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
    "queryid" AS ID,
    "text_en" AS Query,
    "sub_collection" AS Zeitpunkt 
FROM "Topic"
WHERE "sub_collection" = '2023-01';
'''

df = pd.read_sql(query, con=engine)


# ---> Genereal Purpose LLM, um die zugehörigen Stadien zu wissen
openai.api_key = "KEY"

def get_infos(query):
    prompt = f"You have this Query. Give a score on how time dependant this Query:{query}.The Score is between 0 to 1. Don't answer with anything more than the Score."
    response = openai.chat.completions.create(
        model="gpt-4o-mini", 
        messages=[{"role": "user", "content": prompt}],
        max_tokens=50, #---> Es wird nur nach dem Stadionnamen gefragt
        temperature=0 #---> Eindeutige deterministische Antworten verlangt
    )
    tag = response.choices[0].message.content.strip() #---> Fügt die Antwort zu einem String zusammen
    print(f"Processed Query: {query} -> Tag: {tag}") # ---> Ausgabe um den Prozess zu verfolgen
    return tag
df['tag'] = df['query'].apply(get_infos)
output_file = "Expanded/Query_Expansion_231.csv"
df.to_csv(output_file, index=False)
