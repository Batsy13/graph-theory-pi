import os
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("DATABASE")
password = os.getenv("PASSWORD")

NEO4J_URI = os.getenv("URI")
NEO4J_AUTH = (database_url, password)

API_URL_DEPUTIES = "https://dadosabertos.camara.leg.br/api/v2/deputados"

def create_deputy_constraint(driver):
    print("Ensuring uniqueness constraint for :Deputy(id)...")
    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT IF NOT EXISTS FOR (d:Deputy) REQUIRE d.id IS UNIQUE
        """)
    print("Constraint ensured.")

def fetch_deputy_data_from_api():
    print(f"Fetching data from {API_URL_DEPUTIES}...")
    try:
        params = {'formato': 'json', 'itens': 600}
        response = requests.get(API_URL_DEPUTIES, params=params, timeout=30)
        response.raise_for_status()
        print("Data successfully received from API.")
        return response.json()["dados"]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def inject_deputies_into_neo4j(driver, deputy_list):
    total = len(deputy_list)
    print(f"Injecting/Updating {total} deputies into Neo4j...")
    
    # 1. MERGE encontra um nó :Deputado com o 'id' correspondente ou cria um novo se não existir.
    # 2. SET d += $props atualiza/adiciona todas as propriedades do deputado.
    query = """
    MERGE (d:Deputy {id: $deputy_props.id})
    SET d += $deputy_props
    """

    for i, deputy_data in enumerate(deputy_list):
        with driver.session() as session:
            session.run(query, deputy_props=deputy_data)
        
        print(f"  -> Processed {i + 1}/{total}: {deputy_data['nome']} ({deputy_data['siglaPartido']})")

    print("Deputy injection completed successfully!")


def main():
    
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
            driver.verify_connectivity()
            print("Connection to Neo4j established successfully.")
            
            create_deputy_constraint(driver)
            
            deputy_list = fetch_deputy_data_from_api()
            
            if deputy_list:
                inject_deputies_into_neo4j(driver, deputy_list)
            else:
                print("No deputy data to inject. Shutting down.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()