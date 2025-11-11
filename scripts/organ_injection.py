import os
import requests
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
database_url = os.getenv("DATABASE")
password = os.getenv("PASSWORD")

NEO4J_URI = os.getenv("URI")
NEO4J_AUTH = (database_url, password)
API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

def create_organ_constraint(driver):
    print("Ensuring uniqueness constraint for :Organ(id)...")
    with driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (o:Organ) REQUIRE o.id IS UNIQUE")
    print("Constraint ensured.")

def ingest_all_organs(driver):
    print("Starting organ ingestion...")
    all_organs = []
    endpoint = f"{API_BASE_URL}/orgaos"
    params = {'itens': 100}
    
    while endpoint:
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_organs.extend(data["dados"])
            
            next_link = next((link for link in data['links'] if link['rel'] == 'next'), None)
            if next_link:
                endpoint = next_link['href']
                params = {}
            else:
                endpoint = None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching organs: {e}")
            endpoint = None
            
    if all_organs:
        print(f"Ingesting/Updating {len(all_organs)} organs into Neo4j...")
        query = "UNWIND $organs AS organ_props MERGE (o:Organ {id: organ_props.id}) SET o += organ_props"
        with driver.session() as session:
            session.run(query, organs=all_organs)
        print("Organ ingestion complete.")

def link_votations_to_organs(driver):
    print("Linking votations to organs...")
    query = """
    MATCH (v:Votation)
    WHERE v.uriOrgao IS NOT NULL
    WITH v, toInteger(split(v.uriOrgao, '/')[-1]) AS organ_id
    MATCH (o:Organ {id: organ_id})
    MERGE (v)-[:OCCURRED_IN]->(o)
    """
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
        print(f"Created {summary.counters.relationships_created} OCCURRED_IN relationships for votations.")

def get_all_deputy_ids(driver):
    with driver.session() as session:
        result = session.run("MATCH (d:Deputy) RETURN d.id AS id")
        return [record['id'] for record in result]

def link_deputies_to_organs(driver):
    print("Fetching all deputy IDs from database...")
    deputy_ids = get_all_deputy_ids(driver)
    total_deputies = len(deputy_ids)
    print(f"Found {total_deputies} deputies. Starting to link them to organs...")
    
    query = """
    MATCH (d:Deputy {id: $deputy_id})
    UNWIND $memberships AS membership
    MATCH (o:Organ {id: membership.idOrgao})
    MERGE (d)-[r:MEMBER_OF]->(o)
    SET r.title = membership.titulo, 
        r.startDate = membership.dataInicio, 
        r.endDate = membership.dataFim
    """
    
    for i, deputy_id in enumerate(deputy_ids):
        endpoint = f"{API_BASE_URL}/deputados/{deputy_id}/orgaos"
        try:
            response = requests.get(endpoint, timeout=30)
            response.raise_for_status()
            data = response.json()
            memberships = data.get("dados", [])
            
            if memberships:
                with driver.session() as session:
                    session.run(query, deputy_id=deputy_id, memberships=memberships)
                print(f"  -> {i+1}/{total_deputies}: Linked deputy {deputy_id} to {len(memberships)} organs.")
            else:
                print(f"  -> {i+1}/{total_deputies}: Deputy {deputy_id} has no organ memberships.")
            
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"  -> {i+1}/{total_deputies}: Error fetching organs for deputy {deputy_id}: {e}")

def main():
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
            driver.verify_connectivity()
            print("Successfully connected to Neo4j.")
            
            create_organ_constraint(driver)
            
            ingest_all_organs(driver)
            
            link_votations_to_organs(driver)
            
            link_deputies_to_organs(driver)
            
            print("\nOrgan ingestion and linking process completed!")
            
    except Exception as e:
        print(f"A general error occurred: {e}")

if __name__ == "__main__":
    main()