import logging
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 :
        return func.HttpResponse("Au moins une des variables d'environnement (pour la base de donnée SQL) n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tGenres.genre, AVG(tTitles.averageRating) AS averageGenreRate FROM tGenres JOIN tTitles ON tTitles.tconst = tGenres.tconst WHERE tTitles.averageRating IS NOT NULL GROUP BY tGenres.genre ORDER BY averageGenreRate DESC")
            rows = cursor.fetchall()
            for row in rows:
                dataString += f"Requesting Average Rate By Genre Using SQL: \n genre={row[0]}, averageGenreRate={row[1]}\n"
    except:
        errorMessage = "Erreur de connexion a la base SQL"
        
    if errorMessage != "":
        return func.HttpResponse(dataString + nameMessage + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + nameMessage + " Connexions réussies a SQL!")