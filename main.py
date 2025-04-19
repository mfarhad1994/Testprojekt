# Dies ist für FastAPI Anwendung. Sie steuert:
# 1)  Scraping von Pressemitteilungen,
# 2)  Klassifizierung der gescrapten Daten,
# 3)  Speichern der klassifizierten Daten in PostgreSQL.


import os  
import  uvicorn  # Dient zum Starten von FastAPI-Servers
import json  
from fastapi import FastAPI  # FastAPI ist  Framework für  API-Endpunkte
import psycopg2  # Zum Herstellen Verbindung zu PostgreSQL-Datenbanken

import asyncio  # Unterstützt asynchrone Programmierung.

import random  
from  datetime import  datetime  
import httpx  # Für asynchrone HTTP-Anfragen in Python.


# Importieren von Funktionen aus scraper_functions.py
from scraper_functions import (
    load_visited_urls,
    save_visited_urls ,
    save_results_to_file,
    get_article_list_page,
    get_articles_on_page ,
    extract_article_data,
    maybe_pause,
    log_scrape_status ,
    append_unscraped_urls ,
    extract_article_by_url,
    merge_jsonl_files_deduplicated )


# Importieren von Funktionen aus elastic_search.py
from elastic_search import (
    train_and_index_training_data,
    classify_extracted_data )


# Initialisiert  FastAPI
app = FastAPI()


# Artikel,  älter als  dieses Datum sind,  werden abgebrochen.
CUT_OFF_DATE=datetime( 2024, 10, 3)

# Basisteil der URL 
BASE_URL= "https://www.lifepr.de"


@app.get("/scrape")
async def scrape_lifepr_articles( max_pages:int= 1000 ):
    """
    Diese Funktion wird über Endpunkt /scrape aufgerufen.
    Sie ruft bis zu max_pages Seiten auf lifepr.de auf,
    extrahiert alle Pressemitteilungen und speichert Ergebnisse in JSONL.
    """
    # Geladene, bereits besuchte URLs werden als Set gespeichert, um duplikate zu vermeiden
    visited_urls=  load_visited_urls( "visited_urls.txt")
    # all_results sammelt  Daten von aller erfolgreich gescrapten Artikel
    all_results=[]

    # Erstellen  asynchronen HTTPX-Clients mit  Timeout 
    async with  httpx.AsyncClient( timeout =30.0) as client :
        stop_scraping=False  # Variable, um  scraping abzubrechen, wenn alte Artikel vorkommen
        page =1  # Start mit Seite 1

        # Solange  Seitenzahl <= max_pages ist , nicht abbrechen 
        while page<= max_pages  and not stop_scraping :
            # Erzeugt  URL für  aktuelle Seite
            url=get_article_list_page( page )
            

            # Ruft asynchron  Artikel auf dieser Seite ab 
            articles, _ =  await get_articles_on_page( url ,client)
            if not articles :
                # Falls keine Artikel gefunden wurden, abbrechen
                
                break

            # tasks speichert einzelne Aufgaben , um alle Artikel von seite zu scrapen
            tasks =[]
            for art in  articles:
                # Für jeden Artikel (art) wird eine asynchrone funktion extract_article_data erzeugt
                tasks.append( extract_article_data( art,BASE_URL, visited_urls,CUT_OFF_DATE,  client  ) )
                
                    
                

            # gather führt  asynchronen Aufgaben parallel aus
            results=await asyncio.gather( *tasks, return_exceptions =True)
            # Hier werden gescheiterte URLs gespeichert, um  später wieder zu versuchen
            failed_urls= []

                 # itirieren alle Ergebnisse  von asynchronen tasks
            for i,data in enumerate( results) :
                if isinstance(data , Exception):
                    # Falls  unerwarteter Fehler in extract_article_data aufgetreten ist
                    
                    continue

                if data is  None:
                    # Wenn None, konnte  nichts extrahiert werden
                    try :
                        # Versuche,  URL aus  <article> zu erhalten und in failed_urls hinzuzufügen
                        article_a= articles[i].find( "a")
                        if  article_a:
                            fail_url=article_a.get("href" , "" )
                            # Wenn  URL nicht mit http beginnt, base-URL dazufügen
                            if fail_url and not  fail_url.startswith( "http" ):
                                fail_url =BASE_URL  + fail_url
                            failed_urls.append( fail_url )
                    except:
                        pass
                    continue

                if "STOP_SCRAPING" in data:
                    # Falls   Datum zu alt ist (älter als CUT_OFF_DATE), wird   Scraping abgebrochen
                    stop_scraping =True
                    
                    break

                # Liegt kein Fehler oder STOP-SCRAPING vor, fügen wir   Ergebnis zu all_results hinzu
                all_results.append( data)

            # Speichert  aktualisierte Liste von besuchter URLs in visited_urls.txt
            save_visited_urls( visited_urls ,"visited_urls.txt" )

            #  Status dieser Seite: welche URLs besucht und welche fehlgeschlagen sind
            log_scrape_status( page , visited_urls , failed_urls)
            # Schreiben alle fehlgeschlagenen URLs in unscraped_urls.txt
            append_unscraped_urls( failed_urls )

            # Führt eine zufällige Pause ein
            maybe_pause(page)

            # Erhöht  Seitenzahl, um zu nächsten Seite zu gehen
            page +=1
            # Wartet asynchron für eine zufällige Zeit 
            await asyncio.sleep(random.uniform (1.0 , 2.0) )

    
     #Nach  Iteration aller Seiten: Speichern  erfolgreichen results in extracted_data.jsonl
    save_results_to_file( all_results,"extracted_data.jsonl")
    

    # Falls  unscraped_urls.txt existiert, versuchen wir  erneuten Abruf
    if os.path.exists( "unscraped_urls.txt"  ):
        with open( "unscraped_urls.txt", "r" ,encoding="utf-8")  as f:
            
            retry_urls= [line.strip()  for line in f  if line.strip() ]

        

        async with  httpx.AsyncClient( timeout =30.0) as  retry_client :
            # Erstellen Liste von asynchronen Tasks für fehlgeschlagenen URL
            retry_tasks = [ extract_article_by_url (url , visited_urls ,CUT_OFF_DATE, retry_client)
                            for url in  retry_urls  ]
               
            
            retry_results = await asyncio.gather(*retry_tasks , return_exceptions =True )

        # retry_successes sammelt  alle erfolgreischen Ergebnisse  bei zweiten Versuch
        retry_successes= []
        for data in  retry_results :
             # Falls erneut Exception oder None zurückkommt, ignorieren wir Ergebnis
            if isinstance( data , Exception) or data is  None:
                continue
            # STOP_SCRAPING (zu alt) überspringen wir auch
            if "STOP_SCRAPING"  in data :
                continue
            retry_successes.append(data)

        #Wenn  wir beim zweiten Versuch noch erfolgreiche Artikel gefunden haben, speichern wir 
        if retry_successes:
            save_results_to_file( retry_successes , filename ="extracted_data_failed.jsonl")
            

    #  Zum Schluss werden  extracted_data.jsonl und extracted_data_failed.jsonl
     # zusammengeführt und doppelte Einträge anhand  der PM_URL entfernt.
    merge_jsonl_files_deduplicated( [ "extracted_data.jsonl" ,"extracted_data_failed.jsonl"],"extracted_data_final.jsonl" )
       
   
    

    # kurze Zusammenfassung 
    return {"status": "OK" ,
            "pages_scraped" : page - 1,
            "articles_scraped": len( all_results),}
        
    


@app.get("/classify")
def classify_all():
    """
    Dieser Endpunkt führt  folgenden Schritte durch:
    1) Erzeugt einen Index training_data_index in Elasticsearch und indexiert training_data.jsonl,
    2) Klassifiziert extracted_data_final.jsonl und speichert   Ergebnis in extracted_data_classified.jsonl,
 
    """
    # Schritt 1 : Trainingsdaten einlesen und in Elasticsearch indexieren
    train_and_index_training_data(training_json="training_data.jsonl" ,index_name="training_data_index"  )
        
    

    #Schritt 2: Klassifizierung alle gesammelten Daten
    classify_extracted_data(extracted_json="extracted_data_final.jsonl"  , output_json="extracted_data_classified.jsonl",
                            index_name="training_data_index")
        
    

    # Rückgabe einer Bestätigung
    return {"status": "OK"  ,  "message": "Training & classification done. See extracted_data_classified.jsonl"}
        
    






# Datenbankverbindungs-URL 
DATABASE_URL=  "postgresql://myuser:mypass@localhost:5432/mydb"


# SQL command, um  Tabelle zu erstellen, falls sie noch nicht existiert.
CREATE_TABLE_SQL=  """
CREATE TABLE IF NOT EXISTS extracted_articles (
    PM_Datum DATE,
    PM_Headline TEXT ,
    PM_URL TEXT PRIMARY KEY,
    Unternehmen TEXT,
    "Strasse 1" TEXT,
    PLZ TEXT,
    Ort TEXT ,
    Land TEXT,
    "Webseite URL" TEXT,
    "Email 2" TEXT,
    "Telefon 2" TEXT,
    Anrede TEXT,
    Grad TEXT ,
    Vorname TEXT ,
    Nachname TEXT,
    Position TEXT,
    "Email 1" TEXT,
    "Telefon 1" TEXT,
    Branche TEXT );
"""


# SQL command, um  Datensatz in  Tabelle einzufügen.
# Falls PM_URL bereits existiert (PRIMARY KEY), wird  Eintrag ignoriert.
INSERT_SQL = """
    INSERT INTO extracted_articles (
        PM_Datum, PM_Headline, PM_URL, Unternehmen, "Strasse 1", PLZ, Ort, Land, "Webseite URL",
        "Email 2", "Telefon 2", Anrede, Grad, Vorname, Nachname, Position,
        "Email 1", "Telefon 1", Branche    )
        VALUES (%s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s )
        ON CONFLICT (PM_URL) DO NOTHING;
"""


@app.get("/store")
def store_classified_data_in_db():
    # klassifizierten JSONL soll  in PostgreSQL geladen werden 
    file_path="extracted_data_classified.jsonl"
    if not os.path.exists( file_path ):
        # Falls  Datei nicht existiert ---> Fehler
        return {"status": "error"  , "message": "File not found: extracted_data_classified.jsonl" }

    # Verbindungsversuch mit PostgreSQL
    try :
        conn= psycopg2.connect(DATABASE_URL )
        cur= conn.cursor()
        # Führt  SQL command aus, der  Tabelle erstellt, falls sie nicht existiert.
        cur.execute( CREATE_TABLE_SQL ) 
        conn.commit()
    except Exception as e:
        
        return {"status": "error", "message": f"Could not connect or create table: {e}"}

    # Zählt, wie viele Datensätze erfolgreich eingefügt werden
    records_inserted= 0

    
    with open(file_path , "r",encoding="utf-8" ) as f:
        for line in  f :
            try:
                
                item =json.loads(line )
            except json.JSONDecodeError:
                continue

            # Extrahiert  Werte aus dem JSON in  Tupel, passend zur Reihenfolge der Spalten
            row = ( item.get("PM_Datum", ""),
                    item.get("PM_Headline", ""),
                    item.get("PM_URL", "") ,
                    item.get("Unternehmen", ""),
                    item.get("Strasse 1" , ""),
                    item.get("PLZ", ""),
                    item.get("Ort", "") ,
                    item.get("Land", "") ,
                    item.get("Webseite URL", ""),
                    item.get("Email 2" , ""),
                    item.get("Telefon 2", ""),
                    item.get("Anrede" , ""),
                    item.get("Grad" , "") ,
                    item.get("Vorname" , ""),
                    item.get("Nachname", ""),
                    item.get("Position", ""),
                    item.get("Email 1", ""),
                    item.get("Telefon 1", ""),
                    item.get("Branche", "")  )
            
          

            try:
                # Führt  INSERT command mit  extrahierten Werten aus
                cur.execute( INSERT_SQL , row)
                records_inserted +=1  # Erhöht  Zähler bei erfolgreicher Einfügung
            except Exception  as e:
                # Falls ein Fehler auftritt , Zeile überspringen
                continue

    # Übernimmt alle ausgeführten SQL-Änderungen in  Datenbank
    conn.commit()
    # Schließt Cursor und Datenbankverbindung
    cur.close()
    conn.close()

    # Gibt eine Zusammenfassung zurück
    return { "status": "OK" ,  "message": f"Inserted {records_inserted} records into 'extracted_articles' table."}
       
    



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
