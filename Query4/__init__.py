import logging
import os
import pyodbc as pyodbc
import azure.functions as func

def get_query_condition(genre, actor, director) : 
    query = ""
    if(len(actor)!=0):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tPrincipals WHERE tPrincipals.nconst = '{actor}' AND tPrincipals.category = 'acted in')"
    if(len(director)!=0):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tPrincipals WHERE tPrincipals.nconst = '{director}' AND tPrincipals.category = 'directed')"
    if(len(genre)!=0):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tGenres WHERE genre='{genre}')"
    return query

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    actor = req.params.get('actor')
    director = req.params.get('director')

    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 :
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT AVG(tTitles.runtimeMinutes) AS averageRuntime FROM tTitles WHERE runtimeMinutes IS NOT NULL {get_query_condition(genre, actor, director)}")

            rows = cursor.fetchall()
            dataString=f"le temps moyen des films respectant les critères suivant acteur={actor if len(actor)!=0 else 'ALL'}, directeur={director if len(director)!=0 else 'ALL'}, genre={genre if len(genre)!=0 else 'ALL'} est {rows[0][0]}"

    except
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)
    else:
        return func.HttpResponse(dataString + " Connexions réussie à SQL!")
