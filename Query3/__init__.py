import logging
from py2neo import Graph
import os
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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
        artistsAndMovies = graph.run("MATCH (t1:Title)<-[r1]-(n:Name)-[r2]->(t2:Title) WHERE type(r1)<>type(r2) AND t1.tconst=t2.tconst RETURN DISTINCT n.primaryName, t1.primaryTitle ORDER BY n.primaryName")
        dataString="Requesting artists playing two roles in the same movie Using Cypher"
        for row in artistsAndMovies:
            dataString += f"primaryName={row['n.primaryName']}, primaryTitle={row['t1.primaryTitle']}\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
          
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexion réussie a Neo4j")
