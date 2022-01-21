import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
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

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        listByGenre = graph.run("Match (t:Film)-[:IS_OF_GENRE]-(g:Genre) WITH g, collect(t.idFilm) AS movies_collection RETURN g.genre, movies_collection")
        try:
            logging.info("Test de connexion avec pyodbc...")
            with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                for listOfMovies in listByGenre :
                    cursor = conn.cursor()
                    formattedMoviesList = f"{listOfMovies[1]}".replace("[",'(').replace("]",")") 
                    cursor.execute(f"SELECT AVG(runtimeMinutes) FROM tTitles WHERE runtimeMinutes IS NOT NULL AND tconst IN {formattedMoviesList}")
                    rows = cursor.fetchall()
                    dataString += f"genre= {listOfMovies[0]} averageRuntime={rows[0][0]} \n"
        except:
            errorMessage = "Erreur de connexion a la base SQL"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"        
       
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + "\nConnexions réussies a Neo4j et SQL!")
