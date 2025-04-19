#  dieses Modul enthält Funktionen für  Scraping und Parsing von Daten 


import os  
import json 
import  re   # Mustererkennung 
import time  # zeitbezogene  Funktionen (z.B. sleep).
import  random  
import asyncio  # Fur  asynchrone Abläufe.
from datetime import datetime  
from  bs4 import BeautifulSoup  #  Parsing von  HTML-Inhalten 

import   httpx  #  für asynchrone  HTTP-Anfragen.


from selenium.webdriver.common.by  import  By  #  Auswahl von  HTML elementen nach verschiedenen Attributen.
from selenium.webdriver.support.wait import WebDriverWait  # wartet auf  bestimmte Bedingungen .
from selenium.webdriver.support  import expected_conditions as  EC  # Bietet übliche  Wartebedingungen.

from selenium  import webdriver  # zum Steuern von Webbrowsern.
from selenium.webdriver.chrome.options import  Options  # Stellt  Konfigurationsoptionen für den Chrome-Browser bereit.



#  unterschiedliche  Browser und Geräte zu simulieren.

    
user_agents =    ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                "Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.112 Mobile Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:123.0) Gecko/20100101 Firefox/123.0"]


def get_rotating_headers():
    """  Wählt zufällig einen User-Agent"""

    return {"User-Agent": random.choice(user_agents), "Accept-Language": "de-DE,de;q=0.9,en;q=0.8", 
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",}
        
    




def  handle_rate_limit ( response):
    """
    Wenn Statuscode 429 ("Too Many Requests") vorkommt,wird gewartet, bevor es weitergeht.
    """
    if response.status_code == 429:  
        retry_after  = response.headers.get( "Retry-After" )  # Prüfen, ob ein Wartezeitwert gegeben ist
        # Wenn retry_after gültig ist, wird er genutzt; sonst Zufallswert zwischen 60 und 90 Sekunden.
        wait_time = int(retry_after ) if  retry_after and  retry_after.isdigit() else  random.randint( 60,90)
        
        time.sleep (wait_time )  #  pausieren
        return  True
    return   False

def maybe_pause (page):
    """
     nach einigen Seiten  längere Pause ,um  auffälliges Verhalten oder Serverüberlastung zu vermeiden. 
    """
    if  page % 5 ==0:  # nach jede 5 Seiten
        pause_time  = random.uniform( 30, 65)  # Zufällige Wartezahl zwischen 30 und 65
       
        time.sleep( pause_time )  # Für  se  Dauer warten






def flatten_contact_field( field_dict):
    """
    Werte von dictionary oder leere string. Wenn es mehrere Ansprechpartner gibt,
    werden sie als 'Person 1', 'Person 2' usw. zusammengefasst.
    """
    if not any ( v for v in  field_dict.values() ): # Prüfen, ob alle Werte im Wörterbuch leer sind
        return ""

    parts= []
    for k,v in  field_dict.items(): #Baut string aus key-value pairs auf. list Werte werden mit Kommas verknüpft 
        if  isinstance(v, str):
            parts.append(f"{k}: {v}")
        elif isinstance( v, list) :
            parts.append(f"{k}: {', '.join(v)}" )

    return  ", ".join( parts )


def load_visited_urls (path=  "visited_urls.txt"):
    """
    einlesen bereits besuchter URLs. So wird vermieden, dass   gleiche URL mehrfach gescrapt wird.
    """
    if os.path.exists( path):  # Prüfen, ob Datei existiert
        with open(path, "r", encoding="utf-8") as f:
            return   set(line.strip() for  line in f if line.strip() ) 
    return  set()  # Wenn  Datei nicht existiert, -->  leere set 


def save_visited_urls( visited_urls,path = "visited_urls.txt"):
    """
       Speichert besuchter URLs in set 
    """
    with  open(path, "w",encoding="utf-8") as f :
        for url in visited_urls:
            f.write( url + "\n")


def save_results_to_file( results,  filename ="extracted_data.jsonl"):
    """
    Speichert eine Liste von Ergebnis als dictionaries in jsonl
    
    """
    with open( filename, "w",encoding="utf-8")   as f :
        for item in  results:
            f.write(json.dumps(  item, ensure_ascii =False) + "\n")




def get_article_list_page( page :int)  ->   str:
    """
    Erzeugt URL für erste und nächste Seite mit Pressemitteilungen. Letzter Tag und aktive Pressemitteilungen sind in der URL definiert
    """
    base_url=   "https://www.lifepr.de"
    if  page  == 1 :
        return    f"{base_url}/pressemitteilung/suche/bis/2025-04-03/sprache/de/inaktive-stories/nicht-anzeigen/inhaltsarten/pressemitteilung"
    else:
        return f"{base_url}/pressemitteilung/suche/bis/2025-04-03/sprache/de/inaktive-stories/nicht-anzeigen/inhaltsarten/pressemitteilung/seite/{page}"


async def  get_articles_on_page(url: str,client: httpx.AsyncClient ):
    """
     Sendet  asynchron  Anfrage an   Seite mit Pressemitteilungslisten, parst  se mit BeautifulSoup 
    """
    # HTTP GET-Anfrage mit rotierenden Headers
    resp= await client.get( url, headers= get_rotating_headers(), timeout=30.0 )
    # Inhalt mit BeautifulSoup parsen
    soup =  BeautifulSoup(resp.text, "html.parser")
    # Alle <article> mit data-unn-component="press-release.teaser" suchen
    articles =  soup.find_all( "article", {"data-unn-component": "press-release.teaser"} )
   
    return articles,soup


def do_selenium_scrape( pm_url , driver):
    """
    mit Selenium, Cookies  zu akzeptieren, zu scrollen und mögliche E-Mails im text zu finden.
    """
    driver.get( pm_url )  # Öffnen  URL im Browser

    try:
        # Wartet bis zu 10 Sekunden  und klickt Cookie Accept button
        WebDriverWait(driver , 10).until (
            EC.element_to_be_clickable( (By.CSS_SELECTOR,"button[data-accept-action='all']") )).click()
        
    except:
        pass  # Wenn kein Cookie button gefunden , ignorieren

    # Scroll zum Ende von seite, um dynamische Inhalte zu laden
    driver.execute_script ("window.scrollTo(0, document.body.scrollHeight);" )
    time.sleep(2)  # Wartet 2 Sekunden

    try:
        # Wartet auf itemprop='email'
        WebDriverWait(driver,10).until(
            EC.presence_of_element_located( (By.CSS_SELECTOR, "[itemprop='email']")) )
       
    except:
        pass  # Wenn  element nicht erscheint, weitermachen

    #  ganzen  HTML-Code holen
    page_source =driver.page_source

    # finden strukturierte Firmen e-mail mit itemprop='email' 
    try:
        company_email= driver.find_element( By.CSS_SELECTOR,"[itemprop='email']").text
    except:
        company_email= ""

    # Mithilfe  Regex alle E-Mail Muster im quelltext finden
    all_emails_list= re.findall( r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",page_source)
    all_emails_list= list(set( all_emails_list ) )  # entfernen doppelte  durch set
    # Entfernen e-mails von 'lifepr.de' und  bereits gefundene  Firmen E-Mail
    all_emails_list = [  e for  e in all_emails_list  if not e.lower().endswith("@lifepr.de")  and e.lower() !=  company_email.lower()]
       
        
       
    
    # Verknüpften alle  E-Mails zu  string
    if  not all_emails_list:
        all_emails_str  = ""
    else :
        all_emails_str  = ", ".join( all_emails_list )

    return company_email,all_emails_str


def log_scrape_status( page_num,visited_urls_on_page,  failed_urls_on_page, log_path ="scrape_status.jsonl"):
    """
    Speichen seitenzahl, besuchten URLs und fehlgeschlagenen URLs.
    """
    entry= { "page":  str(page_num ),
        "visited_urls": list( visited_urls_on_page ),
        "not_scraped_urls":  list(failed_urls_on_page)}
        
    
    
    with open(log_path, "a",encoding="utf-8")  as f:
        f.write(json.dumps( entry, ensure_ascii=False) + "\n" )


def append_unscraped_urls(failed_urls_on_page, path= "unscraped_urls.txt"):
    """
    Fügt alle fehlgeschlagenen URLs in  Text file hinzu
    """
    with open(path, "a",encoding="utf-8") as f:
        for url in  failed_urls_on_page :
            f.write(url + "\n" )


async def extract_article_data( art,base_url, visited_urls,  cutoff_date,client: httpx.AsyncClient):


    """
    Nimmt ein <article> element und extrahiert alle benötigten Daten:
     - Headline, URL
     - Prüft, ob der Artikel neu ist
     - Scrapen Datum, Text, Kontakte usw.
     - Gibt  dictionary mit allen Feldern 
    """
    # Sucht  <h1> element mit  class 'h3' im Article
    h1_tag= art.find( "h1", class_ ="h3")
    pm_url  = ""
    if  h1_tag:
        # Holt  <a> element
        headline_link =  h1_tag.find("a")
        if  headline_link:
            # Extrahiert  href und stellt sicher, dass es mit http beginnt
            pm_url =  headline_link.get( "href", "" ).strip()
            if pm_url and not pm_url.startswith( "http") :
                pm_url= base_url + pm_url

    if not pm_url:
        # Wenn keine URL gefunden wird,  Artikel überspringen
        return None

    # Wenn  URL bereits besucht wird, überspringen.
    if  pm_url in visited_urls:
        
        return None

    # Fügt  URL zur Liste von  besuchter seiten hinzu, um doppelungen zu verhindern
    visited_urls.add(pm_url)

    try:
        # Mit httpx   Artikelseite abrufen
        resp_art =  await  client.get(pm_url, headers=get_rotating_headers(), timeout=30.0 )
    except  Exception as e:
        
        return  None

    # HTML von Artikelseite parsen
    soup_art= BeautifulSoup(resp_art.text, "html.parser" )

    # Headline extrahieren
    headline_tag =soup_art.find( "h1", itemprop="headline")
    pm_headline = headline_tag.get_text(strip=True)   if headline_tag else  ""

    # Publikationsdatum aus  <time> tag holen
    time_tag =  soup_art.find( "time",itemprop="datePublished")
    if not time_tag:
        # Ohne Datum --> None
        return  None
    pm_datum=time_tag.get( "datetime", "" ).split("T")[0]

    # Datum string in  datetime umwandeln
    pm_date_obj = datetime.strptime( pm_datum, "%Y-%m-%d" )
    # Mit  cutoff_date vergleichen
    if  pm_date_obj < cutoff_date :
        return  {"STOP_SCRAPING":True}

    # Haupttext von Artikel
    article_body=  soup_art.find("div",{"itemprop": "articleBody"} )
    article_text=  article_body.get_text( strip=True )   if article_body else  ""

    # Firmenname nehmen
    unternehmen_tag = soup_art.find( "span",  itemprop="name legalName")
    unternehmen =unternehmen_tag.get_text( strip=True )    if unternehmen_tag else ""

    # Adressdaten extrahieren
    address_art = soup_art.find("address", itemprop="address")
    if address_art :
        street_tag =  address_art.find( "span", itemprop="streetAddress" )
        plz_tag= address_art.find( "span", itemprop="postalCode")
        city_tag = address_art.find("span", itemprop="addressLocality" )
        street_art = street_tag.get_text(strip=True )    if street_tag else ""
        plz_art  =  plz_tag.get_text(strip=True) if  plz_tag else ""
        city_art = city_tag.get_text(strip=True) if city_tag  else ""

        # Prüfen, ob sich in  Adresse  'Deutschland' befindet
        country_art =  ""
        for sp in reversed( address_art.find_all("span") ):
            if  "Deutschland" in  sp.get_text(strip=True):
                country_art= sp.get_text(strip=True)
                break
        else:
            #  Falls nicht, meta content prüfen
            meta_country = address_art.find("meta",itemprop="addressCountry" )
            if  meta_country  and meta_country.has_attr ( "content" ):
                country_art =  meta_country ["content"]
    else :
        # Keine Adresse vorhanden ---> leere strings 
        street_art = ""
        plz_art =  ""
        city_art= ""
        country_art =  ""

    # Webseite von Unternehmen extrahieren
    website_tag=  soup_art.find ("a",itemprop="url" )
    website_art= website_tag.get_text( strip=True)   if website_tag else ""

    # Firmen-Telefon holen
    company_phone_art=(soup_art.select_one ('ul.fa-ul.text-body2.cmt-6.mb-0 li a[itemprop="telephone"]' )  or  ""
    ).get_text( strip=True) if  soup_art.select_one( 'ul.fa-ul.text-body2.cmt-6.mb-0 li a[itemprop="telephone"]')   else ""

    #  Chrome-Session im Headless mode einrichten.
    chrome_options= Options()
    chrome_options.add_argument ("--headless" )  
    local_driver =webdriver.Chrome( options=chrome_options)

    try :
        #Firmen-E-Mail und alle andere E-Mails holen
        loop =  asyncio.get_event_loop()
        company_email_art,all_emails = await  loop.run_in_executor( None,
            lambda: do_selenium_scrape(pm_url, local_driver ))
        
    finally:
        # Driver  beenden
        local_driver.quit()

    # Kontaktinformationen finden
    contact_sections =  soup_art.find_all ( "div", {"data-unn-component": "global.contact-information"} )

    # Wenn es  einen Kontaktabschnitt gibt, Daten für eine person extrahieren
    if len(contact_sections)==1:
        section = contact_sections[0]
        anrede=""
        span_list =section.select( "strong span")
        anrede_text  = span_list[0].get_text(strip=True )   if span_list else ""
        if "Herr"  in anrede_text  or "Frau"  in anrede_text:
            anrede= anrede_text

        grad_tag = section.find("span", itemprop="honorificPrefix" )
        grad= grad_tag.get_text( strip=True) if  grad_tag else ""

        given_tag =section.find("span",itemprop="givenName" )
        given_name = given_tag.get_text(strip= True)  if given_tag else ""

        family_tag = section.find("span" , itemprop="familyName")
        family_name = family_tag.get_text(strip= True)   if family_tag else ""

        job_parts=[]
        for li  in section.find_all("li") :
            spans =  li.find_all("span" )
            for sp in  spans:
                txt = sp.get_text(strip= True)
                # Wenn itemprop fehlt oder itemprop='jobTitle' ist
                if txt  and ("itemprop" not  in sp.attrs or sp.get( "itemprop") =="jobTitle"):
                    job_parts.append(txt)
        job_title= ", ".join( job_parts)   if job_parts else ""

        phone_tags =section.find_all(attrs={"itemprop": "telephone"})
        phone = ", ".join([tag.get_text (strip=True) for tag  in phone_tags]) if phone_tags else ""

    elif  len(contact_sections) >1:
         #  Wenn es mehrere Kontaktabschnitte gibt, sammeln  wir jede person als dictionaries
        anrede={}
        grad={}
        given_name={}
        family_name={}
        job_title={}
        phone={}
         #Person 1: , Person 2:
        for idx , section  in enumerate( contact_sections,start=1):
            key= f"Person {idx}"

            span_list =  section.select("strong span")
            anrede_text  = ""
            for  sp in span_list :
                txt =sp.get_text(strip=True)
                # Prüfen, ob es in Text "Frau" oder "Herr" gibt
                if "Frau" in txt  or "Herr" in txt :
                    anrede_text  = txt
                    break
            anrede[key]= anrede_text

            grad_tag = section.find("span", itemprop="honorificPrefix")
            grad[key] =grad_tag.get_text(strip=True) if grad_tag   else ""

            given_tag =section.find( "span",itemprop="givenName" )
            given_name[key] =given_tag.get_text(strip=True)   if given_tag else ""

            family_tag =  section.find( "span",  itemprop="familyName")
            family_name[key] =  family_tag.get_text(strip=True )    if family_tag else ""

            job_parts=[]
            for li  in  section.find_all( "li"):
                spans= li.find_all ("span")
                for  sp in spans :
                    txt =sp.get_text (strip=True)
                    if txt  and ( "itemprop" not in sp.attrs or   sp.get("itemprop")== "jobTitle") :
                        job_parts.append(txt )
            job_title[key]  = ", ".join( job_parts)    if job_parts else ""

            phone_tags = section.find_all(attrs={"itemprop": "telephone"})
            phone[key] = [tag.get_text(strip=True) for tag in phone_tags] if phone_tags else ""

    else :
        # Wenn kein Kontaktabschnitt gefunden wird --> leere strings 
        anrede = ""
        grad =""
        given_name = ""
        family_name  = ""
        job_title = ""
        phone =""

    # Kategorien  sammeln, falls vorhanden
    kategorien=[]
    kategorien_ol = soup_art.find( "ol",class_= "list-unstyled mb-0 row g-1" )
    if  kategorien_ol :
        li_tags= kategorien_ol.find_all ( "li")
        for li in li_tags:
            spn  = li.find("span",itemprop="articleSection" )
            if  spn:
                txt =  spn.get_text( strip =True)
                #   Wort "pressemitteilung" ausschließen.
                if  txt and txt.lower()  != "pressemitteilung" :
                    kategorien.append (txt)
    kategorien=", ".join( kategorien )   if kategorien else ""

    # Schlagwörter  erfassen
    keywords =[]
    keywords_ol  = soup_art.find ("ol",itemprop= "keywords")
    if keywords_ol :
        li_tags =  keywords_ol.find_all ("li")
        for li in  li_tags:
            spn=li.find( "span")
            if spn:
                txt  = spn.get_text( strip =True)
                if  txt:
                    keywords.append(txt)
    keywords= ", ".join(keywords)    if keywords  else ""

     # dictionaries für mehrperson Kontakte in string umwandeln
    if isinstance( anrede, dict):
        anrede = flatten_contact_field(anrede)
    if isinstance(given_name ,dict):
        given_name= flatten_contact_field(given_name)
    if isinstance( family_name , dict):
        family_name = flatten_contact_field (family_name)
    if isinstance(job_title , dict ):
        job_title=flatten_contact_field(job_title )
    if isinstance(grad, dict):
        grad = flatten_contact_field(grad)
    if isinstance(phone,dict):
        phone= flatten_contact_field (phone)
    if isinstance(company_email_art, dict) :
        company_email_art =  flatten_contact_field( company_email_art)
    # Falls all_emails eine Liste ist, verknüpfen.
    if isinstance(all_emails , list ):
        all_emails= ", ".join( all_emails)

    #   finale dictionary erstellen.
    result = {"PM_Datum": pm_datum,
            "PM_Headline": pm_headline,
            "PM_URL" : pm_url ,
            "Unternehmen": unternehmen,
            "Strasse 1": street_art,
            "PLZ": plz_art ,
            "Ort": city_art,
            "Land" : country_art,
            "Webseite URL": website_art,
            "Email 2": company_email_art,  
            "Telefon 2": company_phone_art,  
            "Anrede": anrede,
            "Grad": grad ,
            "Vorname": given_name,
            "Nachname": family_name,
            "Position": job_title,
            "Email 1": all_emails,  
            "Telefon 1": phone,      
            "Branche" : "" ,           # Es wird später bei  Klassifizierung gefüllt 
            "Kategorien": kategorien,
            "Schlagwörter": keywords,
            "article_text": article_text}
        
    

    return result


async def extract_article_by_url(url,visited_urls, cutoff_date,  client: httpx.AsyncClient ):

    """
    Ähnliche  Funktion wie extract_article_data für erneute Ausprobieren gescheiterter URLs
    """
    # Prüfen, ob   URL bereits bearbeitet ist
    if url  in visited_urls:
        
        return  None

    try:
        # Mit httpx  Seite holen
        resp= await client.get( url, headers=get_rotating_headers(),timeout = 30.0)
    except Exception  as e :
        
        return None

    # HTML von  Artikelseite parsen
    soup_art=BeautifulSoup(resp.text , "html.parser")

    # <time> tag für   Artikeldatum suchen
    time_tag = soup_art.find ("time",itemprop= "datePublished" )
    if not time_tag:
        
        return None
    pm_datum = time_tag.get( "datetime", "").split("T") [0]
    pm_date_obj =datetime.strptime( pm_datum, "%Y-%m-%d" )
    # Wenn es älter als  cutoff ist, nicht weiter verarbeiten
    if  pm_date_obj  < cutoff_date:
        
        return { "STOP_SCRAPING" :True}

    # Headline extrahieren
    headline_tag= soup_art.find ("h1",itemprop= "headline" )
    pm_headline = headline_tag.get_text(strip = True)   if headline_tag else ""

    # Artikeltext extrahieren
    article_body=soup_art.find( "div", {"itemprop": "articleBody"})
    article_text = article_body.get_text(strip=True)   if article_body else ""

    # Firmenname
    unternehmen_tag  = soup_art.find( "span" ,itemprop ="name legalName" )
    unternehmen =   unternehmen_tag.get_text( strip =True)    if unternehmen_tag else ""

    # Adresse
    address_art=soup_art.find ( "address" , itemprop="address")
    if address_art :
        street_tag= address_art.find( "span", itemprop="streetAddress" )
        plz_tag= address_art.find( "span", itemprop="postalCode" )
        city_tag= address_art.find( "span", itemprop="addressLocality" )
        street_art= street_tag.get_text(strip = True)   if street_tag else ""
        plz_art= plz_tag.get_text(strip = True) if plz_tag else ""
        city_art= city_tag.get_text(strip = True)   if city_tag else ""

        country_art= ""
        for sp  in reversed( address_art.find_all( "span")):
            if "Deutschland" in  sp.get_text( strip =True):
                country_art=sp.get_text(strip =True)
                break
            else:
                meta_country = address_art.find ("meta",itemprop="addressCountry" )
                if  meta_country and meta_country.has_attr( "content"):
                    country_art=meta_country[ "content" ]
    else :
        street_art=""
        plz_art=""
        city_art=""
        country_art=""

    # Webseite
    website_tag= soup_art.find( "a" , itemprop="url")
    website_art = website_tag.get_text(strip = True)   if website_tag else ""

    # Firmen-Telefon
    company_phone_art=(
        soup_art.select_one( 'ul.fa-ul.text-body2.cmt-6.mb-0 li a[itemprop="telephone"]')   or ""
    ).get_text(strip= True) if soup_art.select_one('ul.fa-ul.text-body2.cmt-6.mb-0 li a[itemprop="telephone"]')  else   ""

    #  Selenium für  E-Mails 
    chrome_options=Options()
    chrome_options.add_argument("--headless")
    local_driver=  webdriver.Chrome(options = chrome_options)

    try:
        loop = asyncio.get_event_loop()
        company_email_art, all_emails = await loop.run_in_executor(
            None,
            lambda: do_selenium_scrape(url, local_driver)
        )
    finally:
        local_driver.quit()

    # Kontaktinformationen
    contact_sections = soup_art.find_all("div", {"data-unn-component": "global.contact-information"})

    # Einzelkontakt
    if len(contact_sections) == 1:
        section = contact_sections[0]
        anrede=""
        span_list = section.select("strong span")
        anrede_text = span_list[0].get_text(strip=True) if span_list  else ""
        if "Herr"  in anrede_text or "Frau"  in anrede_text:
            anrede= anrede_text

        grad_tag=section.find ("span",itemprop = "honorificPrefix")
        grad = grad_tag.get_text(strip = True)   if grad_tag else ""

        given_tag = section.find("span", itemprop="givenName" )
        given_name =given_tag.get_text(strip = True)   if given_tag else ""

        family_tag= section.find( "span", itemprop="familyName")
        family_name= family_tag.get_text(strip = True) if family_tag else ""

        job_parts= []
        for li in  section.find_all ("li" ):
            spans= li.find_all( "span" )
            for  sp in spans:
                txt =  sp.get_text(strip =True)
                if txt and ( "itemprop" not  in sp.attrs  or sp.get( "itemprop" ) =="jobTitle") :
                    job_parts.append(txt)
        job_title  =  ", ".join(job_parts)   if job_parts else ""

        phone_tags= section.find_all( attrs= {"itemprop": "telephone"})
        phone= ", ".join( [tag.get_text(strip = True)  for tag in  phone_tags])   if phone_tags else ""

    elif  len( contact_sections)  > 1:
        # Mehrere Kontakte
        anrede={}
        grad={}
        given_name = {}
        family_name = {}
        job_title = {}
        phone ={}

        for idx ,section in enumerate (contact_sections, start=1) :
            key= f"Person {idx}"

            span_list=section.select( "strong span")
            anrede_text= ""
            for sp  in span_list :
                txt= sp.get_text (strip= True)
                if "Frau" in  txt or "Herr" in  txt:
                    anrede_text =txt
                    break
            anrede[key]=anrede_text

            grad_tag= section.find( "span",itemprop = "honorificPrefix" )
            grad[key]=grad_tag.get_text (strip = True)   if grad_tag else ""

            given_tag =section.find("span" , itemprop= "givenName")
            given_name[key] =  given_tag.get_text(strip=True)   if given_tag else ""

            family_tag = section.find ("span" ,itemprop= "familyName" )
            family_name[key]=family_tag.get_text(strip=True)   if family_tag else ""

            job_parts= []
            for  li in section.find_all("li" ):
                spans=li.find_all ("span")
                for sp  in spans :
                    txt= sp.get_text(strip= True)
                    if txt  and ("itemprop" not  in sp.attrs  or sp.get("itemprop")== "jobTitle" ) :
                        job_parts.append( txt)
            job_title[key]= ", ".join( job_parts)  if job_parts  else  ""

            phone_tags=section.find_all( attrs= {"itemprop": "telephone"})
            phone[key] =[tag.get_text( strip =True) for  tag in phone_tags ]   if phone_tags else ""

    else:
        # Keine Kontakte
        anrede =""
        grad =""
        given_name = ""
        family_name = ""
        job_title = ""
        phone =""

    # Kategorien
    kategorien= []
    kategorien_ol = soup_art.find("ol" ,class_= "list-unstyled mb-0 row g-1" )
    if kategorien_ol :
        li_tags=kategorien_ol.find_all( "li")
        for  li in li_tags:
            spn  =li.find( "span" ,itemprop ="articleSection")
            if spn:
                txt = spn.get_text(strip=True)
                if txt and txt.lower() != "pressemitteilung":
                    kategorien.append(txt)
    kategorien = ", ".join(kategorien) if kategorien else ""

    # Schlagwörter
    keywords=[]
    keywords_ol= soup_art.find("ol" ,itemprop ="keywords" )
    if keywords_ol :
        li_tags  =keywords_ol.find_all("li" )
        for li  in li_tags:
            spn=li.find( "span")
            if spn :
                txt =spn.get_text( strip=True )
                if  txt:
                    keywords.append( txt)
    keywords= ", ".join(keywords )   if keywords  else ""

    
    if isinstance(anrede, dict):
        anrede =flatten_contact_field(anrede)
    if  isinstance(given_name, dict):
        given_name= flatten_contact_field( given_name)
    if isinstance(family_name,dict):
        family_name = flatten_contact_field(family_name)
    if  isinstance( job_title , dict):
        job_title  = flatten_contact_field(job_title )
    if isinstance(grad, dict) :
        grad=flatten_contact_field(grad)
    if isinstance (phone, dict):
        phone = flatten_contact_field( phone)
    if  isinstance( company_email_art, dict ):
        company_email_art = flatten_contact_field( company_email_art)
    if isinstance(all_emails,list ) :
        all_emails= ", ".join( all_emails)

    # Finale daten als dictionary 
    result = {"PM_Datum" : pm_datum,
            "PM_Headline": pm_headline,
            "PM_URL": url,
            "Unternehmen" :  unternehmen,
            "Strasse 1": street_art ,
            "PLZ":  plz_art,
            "Ort" : city_art,
            "Land":  country_art,
            "Webseite URL" : website_art,
            "Email 2":  company_email_art,
            "Telefon 2": company_phone_art ,
            "Anrede" : anrede ,
            "Grad": grad ,
            "Vorname": given_name ,
            "Nachname": family_name ,
            "Position" : job_title,
            "Email 1":  all_emails ,
            "Telefon 1": phone,
            "Branche": "" ,
            "Kategorien" :  kategorien,
            "Schlagwörter": keywords,
            "article_text": article_text  }
        
    

    return result


def merge_jsonl_files_deduplicated (input_files ,output_file):
    """
    Kombiniert mehrere JSONL, entfernt  duplikate anhand von PM_URL.
    """
    seen_urls=set()  # Hält URLs bereit,  die bereits geschrieben wurden
    with open(output_file,"w",  encoding="utf-8" ) as  fout:
        for file in  input_files :
            # Nur fortfahren, wenn  Datei existiert
            if  os.path.exists( file ):
                with open(file,"r",encoding="utf-8") as fin:
                    for line in fin :
                        try:
                            item= json.loads(line)  # Zeile als JSON parsen
                            pm_url = item.get( "PM_URL")
                            if pm_url and  pm_url not in seen_urls:
                                fout.write( json.dumps(item,ensure_ascii =False) +  "\n" )
                                seen_urls.add(pm_url )  # URL als verwendet kennzeichnen
                        except json.JSONDecodeError:
                            continue  # Fehlerhafte Zeilen überspringen
