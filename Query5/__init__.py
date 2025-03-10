import logging
import os
import pyodbc as pyodbc
import azure.functions as func

def get_query_condition(genre, actor, director) : 
    query = ""
    if(actor):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tPrincipals WHERE tPrincipals.nconst = '{actor}' AND tPrincipals.category = 'acted in')"
    if(director):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tPrincipals WHERE tPrincipals.nconst = '{director}' AND tPrincipals.category = 'directed')"
    if(genre):
        query+= f" AND tTitles.tconst IN ( SELECT tconst FROM tGenres WHERE genre='{genre}')"
    return query

def get_url_param(req, param_name):
    param = req.params.get(param_name)
    if not param:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            param = req_body.get(param_name)
    return param

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = get_url_param(req, 'genre')
    actor = get_url_param(req, 'actor')
    director = get_url_param(req, 'director')

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
            dataString=f"le temps moyen des films respectant les critères suivant acteur={actor if actor else 'ALL'}, directeur={director if director else 'ALL'}, genre={genre if genre else 'ALL'} est {rows[0][0]}mn"

    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)
    else:
        return func.HttpResponse(dataString + "\n Connexion réussie à SQL!")