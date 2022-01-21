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
            cursor.execute("SELECT DISTINCT tGenres.genre FROM tGenres JOIN tTitles ON tTitles.tconst = tGenres.tconst WHERE  tTitles.tconst IN (SELECT tPrincipals.tconst FROM tTitles JOIN tPrincipals ON tTitles.tconst=tPrincipals.tconst JOIN tNames ON tNames.nconst=tPrincipals.nconst GROUP BY tPrincipals.tconst, tPrincipals.nconst HAVING COUNT(DISTINCT tPrincipals.category)>1) ORDER BY tGenres.genre")
            rows = cursor.fetchall()
            dataString = "Requesting Average Rate By Genre Using SQL: \n"
            for row in rows:
                dataString += f"genre={row[0]}\n"
    except:
        errorMessage = "Erreur de connexion a la base SQL"
        
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + "\n Connexion réussie a SQL!")