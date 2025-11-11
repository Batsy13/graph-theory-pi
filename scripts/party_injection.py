import os
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
database_url = os.getenv("DATABASE")
password = os.getenv("PASSWORD")

NEO4J_URI = os.getenv("URI")
NEO4J_AUTH = (database_url, password)

API_URL_PARTIES = "https://dadosabertos.camara.leg.br/api/v2/partidos"

def create_party_constraint(driver):
    print("Ensuring uniqueness constraint for :Party(sigla)...")
    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT IF NOT EXISTS FOR (p:Party) REQUIRE p.sigla IS UNIQUE
        """)
    print("Constraint ensured.")

def fetch_party_data_from_api():
    print(f"Fetching data from {API_URL_PARTIES}...")
    try:
        params = {'formato': 'json', 'itens': 100}
        response = requests.get(API_URL_PARTIES, params=params, timeout=30)
        response.raise_for_status()
        print("Data successfully received from API.")
        return response.json()["dados"]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def inject_parties_into_neo4j(driver, party_list):
    total = len(party_list)
    print(f"Injecting/Updating {total} parties into Neo4j...")
    
    query = """
    MERGE (p:Party {sigla: $party_props.sigla})
    SET p += $party_props
    """

    for i, party_data in enumerate(party_list):
        with driver.session() as session:
            session.run(query, party_props=party_data)
        print(f"  -> Processed {i + 1}/{total}: {party_data['nome']} ({party_data['sigla']})")

    print("Party injection completed successfully!")

def connect_deputies_to_parties(driver):
    print("Starting connection between Deputies and Parties...")
    
    query = """
    MATCH (d:Deputy)
    MATCH (p:Party {sigla: d.siglaPartido})
    MERGE (d)-[:AFFILIATED_WITH]->(p)
    """
    
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
        print(f"Connection completed. {summary.counters.relationships_created} new affiliations were created.")

def main():
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
            driver.verify_connectivity()
            print("Connection to Neo4j established successfully.")
            
            create_party_constraint(driver)
            
            party_list = fetch_party_data_from_api()
            
            if party_list:
                inject_parties_into_neo4j(driver, party_list)
                
                connect_deputies_to_parties(driver)
            else:
                print("No party data to process. Shutting down.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()