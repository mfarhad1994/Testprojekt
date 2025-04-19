#  Hier werden alle Funktionen  bereitgestellt, die eine Verbindung zu Elasticsearch herstellen  und daten klassifizieren.


import os  
import re  
import json  
import numpy as np  
import pandas as pd
from elasticsearch import Elasticsearch  #  Kommunikation mit Elasticsearch 
import tensorflow_hub as hub  #  zum Laden pretrained Modelle von TensorFlow Hub.
import tensorflow_text

from concurrent.futures import ThreadPoolExecutor #  Beschleunigung von  parallelen Ausführung


# Erzeugt eine Verbindung zu Elasticsearch
es=Elasticsearch("http://localhost:9200")

# für den Cache-Pfad von TensorFlow Hub-Modells
os.environ['TFHUB_CACHE_DIR']= './tfhub_models'

# Lädt  Universal Sentence Encoder Multilingual (MUSE), um Texte in Vektoren umzuwandeln
muse=hub.load("https://tfhub.dev/google/universal-sentence-encoder-multilingual/3")


def embed ( text ):
    """
    Erstellt  512-dimensionalen Vektor.
    Gibt NumPy-Array zurück.
    """
    return  muse([ text])[0].numpy()


def clean_text (text):
    """
    Bereinigt und normalisiert Text für späteren Embedding und Indexierung:
    - Alles wird in Kleinschreibung umgewandelt.
    - URLs, Kontrollzeichen und Sonderzeichen  werden entfernt (außer ä, ö, ü, ß)
    - übermäßige Leerzeichen werden reduziert.
    """
    text= text.lower()
    text= re.sub( r"http\S+", "", text )  #  URLs entfernen
    text= re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "",text )  #  Steuerzeichen  entfernen
    text= re.sub(r"[^\w\säöüß]" ,"", text)  # Nur alphanumerische Zeichen und deutsche Umlaute behalten
    text= re.sub( r"\s+", " ", text )  # Mehrfache Leerzeichen zusammenfassen
    return   text.strip()


def create_training_index( index_name ="training_data_index"):
    """
    Erstellt  einen Elasticsearch Index  für training daten. Falls vorhanden, Index wird zunächst gelöscht , und dann mit angegebenen 
    Struktur neu erstellt.
    """
    #  Löscht Index, ignoriert Fehlercode 400 oder 404 (Index nicht vorhanden)
    es.indices.delete( index=index_name,ignore=[400,404])
    

    # Mapping beschreibt die Felder,  Datentypen und wie sie indexiert werden
    mapping_training = {
        "mappings": {
            "properties": {
                "text" :        { "type": "text" },
                "Schlagwoerter": { "type": "text" },
                "Kategorien" :  { "type": "text" },
                "branche":    { "type": "keyword" },
                "embedding": {
                    "type": "dense_vector",
                    "dims" : 512,
                    "index": True,
                    "similarity":  "cosine"
                }
            }
        }
    }
    
    # Erstellt  Index mit  Mapping
    es.indices.create( index=index_name,body = mapping_training )
  


def index_training_data(training_data , index_name="training_data_index" ):
    """
    Nimmt Trainingsdaten und indexiert sie in Elasticsearch. Vor Indexieren werden die Texte bereinigt (clean_text) und 
    dann embedded (embed). Jeder Datensatz sollte die Felder 'text', 'Schlagwörter', 'Kategorien' und 'branche' haben.
    """
    for doc  in training_data:
        # Kombiniert relevante Felder zu  string
        parts = [doc.get("text", ""), doc.get("Schlagwörter", "") ,  doc.get("Kategorien", "")]
 

        
        #Bereinigung und Zusammenfügen
        combined_text =clean_text( " ".join(parts) )

        #Embedding in Vektor und in  list umwandeln
        doc_embedding=embed(combined_text ).tolist()

        # Indexierung in Elasticsearch durchführen
        es.index(index = index_name,           
            document= { "text": doc.get("text", "") ,
                        "Schlagwoerter": doc.get( "Schlagwörter", ""),
                        "Kategorien" : doc.get( "Kategorien" , "" ),
                        "branche": doc.get("branche" , ""),
                        "embedding" : doc_embedding   } )
                
       
         
    # Index aktualisieren,  Einfügungen sichtbar machen
    es.indices.refresh (index=index_name )
 





def train_and_index_training_data( training_json="training_data.jsonl",     # training datei
                                    index_name="training_data_index"    ) :   # Name von ES Index
        

    """
   drei Schritte:Index anlegen, Trainings JSONL laden,  Vektorisieren + Indexieren.
    """
    # Indexstruktur  anlegen
    create_training_index( index_name =index_name)
    

    #   Datei prüfen
    if not  os.path.exists (training_json) :
        
        return

    #  Training daten  einlesen
    with open( training_json, "r" ,encoding="utf-8") as f:
        t_data = [ json.loads(line) for line in f  ]
    


    #  Indexieren aller training datensätze in ES
    index_training_data( t_data , index_name= index_name)
    






def find_similar_branches(article_embedding , k, boost_q_sch,  boost_q_kat, schlagwoerter=None,kategorien=None   ):  
                

    # Wandelt  NumPy in Liste um, weil Elasticsearch JSON erwartet.
    query_vector=article_embedding.tolist()
    
 
    #  should_clauses für keyword boosting 

    should_clauses= []  
    
    if schlagwoerter :  # Nur anlegen, wenn string nicht leer  ist
        should_clauses.append({ "match": {"Schlagwoerter": {"query": schlagwoerter,"boost": boost_q_sch     }}}) #   Boost faktor für Schlagwörter
    
    if kategorien:
        should_clauses.append({"match": { "Kategorien": {"query": kategorien,"boost": boost_q_kat }}})      #  Boost faktor für Kategorien
                      
    
    #  query body für ES
   
    query_body = {
        "size": k,                       # Wie viele hits möchten wir
        "query": {
            "script_score": {            # Hybrid: first bool query, dann Cosine
                "query": {
                    "bool": {
                        "should": should_clauses,
                     
                        "minimum_should_match": 0
                    }
                },
                "script": {
                    # cosine similarity + 1.0 (wird nicht negativ )
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {
                        "query_vector": query_vector
                    }
                }
            }
        }
    }



    # Anfrage an Elasticsearch; Index ist 'training_data_index'
    response = es.search(index="training_data_index", body=query_body)
    
    return response["hits"]["hits"]



def classify_article(article_text,  schlagwoerter ,  kategorien, boost_q_sch,  boost_q_kat,  top_k,   boost_sch,boost_kat  ):
   
    #  Schlagwörter und Kategorien  kann für  Embedding durch Wiederholung  verstärkt werden 
  
    weighted_schlagwoerter= (schlagwoerter  + " ") * boost_sch     if schlagwoerter else ""
    weighted_kategorien=  (kategorien +  " " ) *  boost_kat  if kategorien    else ""

   
    # Kombination & Bereinigung von Input
    
 
    combined_input = clean_text ( " ".join( [article_text , weighted_schlagwoerter, weighted_kategorien ]) )
        
   
    
    #  Embedding 
    article_vec=  embed (combined_input)

    #  Hybrid Suche in Elasticsearch 
    hits = find_similar_branches( article_embedding=article_vec, k= top_k , boost_q_sch=boost_q_sch,boost_q_kat= boost_q_kat,
                                 schlagwoerter=schlagwoerter,kategorien = kategorien)
    

    
    # Falls keine hits--> Branche unbekannt

    if  not hits:
        return "unknown"

 
    # Scores von hits aggregieren --> Voting nach Summen score
   
    branche_scores={}
    for hit in hits :
        branche= hit[ "_source"].get("branche" )
        score= hit["_score" ]
        if  branche:
            branche_scores[ branche]= branche_scores.get( branche , 0)  + score

    #  Höchsten Gesamt score auswählen, sonst 'unknown'
    return   max( branche_scores.items() , key=lambda x : x[1]) [0] if  branche_scores else "unknown"




def classify_and_return(article) :
    """
    Klassifiziert  Artikel und gibt mit erkannten 'Branche' zurück.
    """
    unternehmen= article.get("Unternehmen" , "")
    text= article.get( "article_text" , "")
    schlagwörter= article.get("Schlagwörter" , "")
    kategorien= article.get("Kategorien" , "" )

    branche = classify_article( text,schlagwörter, kategorien, boost_q_sch =2.0,  boost_q_kat=6.0, top_k =7, boost_sch=4,boost_kat=20)

    return  {"Unternehmen": unternehmen,"article_text" : text, "Schlagwörter": schlagwörter, "Kategorien": kategorien,"Branche": branche }



def classify_extracted_data(  extracted_json = "extracted_data_final.jsonl",      # Eingabe
                            output_json="extracted_data_classified.jsonl", # Ausgabe
                            index_name="training_data_index" ):    
   
                  

   
    #  ob Datei existiert
 
    if not os.path.exists(extracted_json) :
        
        return

   
    # extracted_data_final laden 

    with open( extracted_json, "r" ,encoding="utf-8")  as fin :
        original_data= [ json.loads(line)  for  line in fin ]

    # Kopie ohne Vermischung 
    original_data_unmixed=original_data.copy()

   
  
   
    df =pd.DataFrame(original_data)

   
   #Gruppieren nach 'Unternehmen' ---> alle Texte , Schlagwörter , Kategorien pro Firma zusammenfügen
    
    df_grouped = df.groupby("Unternehmen").agg( {  "article_text": lambda x: "   ".join(x.dropna()) ,"Schlagwörter" :lambda x: ", ".join( x.dropna()),
                                                "Kategorien" :   lambda x: ", ".join(x.dropna() )} ).reset_index()
        
       



    # Umwandeln in Liste
    grouped_data=df_grouped.to_dict(orient ="records")

    
    #   parallel klassifizieren 

    with  ThreadPoolExecutor() as executor :
        classified_data= list(executor.map( classify_and_return ,grouped_data ) )

    
    #  mapping zurück auf die Einzel Artikel 
   
    branchen_map= { item[ "Unternehmen" ] : item["Branche"] for  item in classified_data}
    df_unmixed= pd.DataFrame(original_data_unmixed )
    df_unmixed[ "Branche"]= df_unmixed["Unternehmen" ].map( branchen_map)
    df_unmixed = df_unmixed.drop(columns=["article_text", "Kategorien", "Schlagwörter"], errors="ignore")
    # Wandelt mit neuen Branchen zurück in dictionary
    data=df_unmixed.to_dict (orient ="records")

   
    #   Daten als JSONL speichern
  
    with open(output_json, "w" ,encoding="utf-8")  as fout :
        for article in  data:
            fout.write(json.dumps( article, ensure_ascii= False ) + "\n")
