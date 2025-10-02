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

def create_constraints(driver):
    print("Ensuring uniqueness constraints...")
    with driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (v:Votation) REQUIRE v.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Proposition) REQUIRE p.id IS UNIQUE")
    print("Constraints ensured.")

def ingest_initial_votations(driver):
    all_votations = []
    endpoint = f"{API_BASE_URL}/votacoes"
    
    params = {
        'dataInicio': '2025-09-01',
        'dataFim': '2025-09-30',
        'ordem': 'ASC',
        'ordenarPor': 'dataHoraRegistro',
        'itens': 100
    }
    
    print(f"Fetching votations for September 2025 from {endpoint}...")
    
    while endpoint:
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_votations.extend(data["dados"])
            
            next_link = next((link for link in data['links'] if link['rel'] == 'next'), None)
            
            if next_link:
                endpoint = next_link['href']
                params = {} 
            else:
                endpoint = None

        except requests.exceptions.RequestException as e:
            print(f"Error fetching votation list: {e}")
            endpoint = None
            
    if all_votations:
        query = "UNWIND $votations AS votation_props MERGE (v:Votation {id: votation_props.id}) SET v += votation_props"
        with driver.session() as session:
            session.run(query, votations=all_votations)
        print(f"{len(all_votations)} votation nodes were inserted/updated for September 2025.")
    
    return [v['id'] for v in all_votations]

def enrich_votations_with_propositions(driver, votation_ids):
    print(f"\nEnriching {len(votation_ids)} votations with proposition data...")
    
    query = """
    MATCH (v:Votation {id: $votation_id})
    UNWIND $propositions_list AS props
    MERGE (p:Proposition {id: props.id})
    ON CREATE SET p.siglaTipo = props.siglaTipo, p.numero = props.numero, p.ano = props.ano, p.ementa = props.ementa
    MERGE (v)-[:REFERS_TO]->(p)
    """
    
    for i, votation_id in enumerate(votation_ids):
        endpoint = f"{API_BASE_URL}/votacoes/{votation_id}"
        try:
            response = requests.get(endpoint, timeout=30)
            response.raise_for_status()
            detailed_data = response.json()["dados"]
            
            propositions = detailed_data.get("proposicoesAfetadas", []) + detailed_data.get("objetosPossiveis", [])
            
            if propositions:
                with driver.session() as session:
                    session.run(query, votation_id=votation_id, propositions_list=propositions)
                print(f"  -> {i+1}/{len(votation_ids)}: Votation {votation_id} enriched.")
            else:
                print(f"  -> {i+1}/{len(votation_ids)}: Votation {votation_id} has no associated propositions.")
            
            time.sleep(0.2)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching details for votation {votation_id}: {e}")

def link_deputies_to_votes(driver, votation_ids):
    print(f"\nFetching votes and linking deputies for {len(votation_ids)} votations...")
    
    query = """
    MATCH (v:Votation {id: $votation_id})
    UNWIND $votes_list AS vote_info
    MATCH (d:Deputy {id: vote_info.deputado_.id})
    MERGE (d)-[r:VOTED_IN]->(v)
    SET r.vote = vote_info.tipoVoto, r.registrationDate = vote_info.dataRegistroVoto
    """
    
    for i, votation_id in enumerate(votation_ids):
        endpoint = f"{API_BASE_URL}/votacoes/{votation_id}/votos"
        try:
            response = requests.get(endpoint, timeout=30)
            response.raise_for_status()
            votes = response.json()["dados"]
            
            if votes:
                with driver.session() as session:
                    session.run(query, votation_id=votation_id, votes_list=votes)
                print(f"  -> {i+1}/{len(votation_ids)}: {len(votes)} votes linked to votation {votation_id}.")
            else:
                print(f"  -> {i+1}/{len(votation_ids)}: Votation {votation_id} had no individual votes registered.")
            
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching votes for votation {votation_id}: {e}")

def main():
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
            driver.verify_connectivity()
            print("Successfully connected to Neo4j.")
            
            create_constraints(driver)
            
            september_votation_ids = ingest_initial_votations(driver)
            
            if september_votation_ids:
                enrich_votations_with_propositions(driver, september_votation_ids)
                
                link_deputies_to_votes(driver, september_votation_ids)
                
                print("\nVotation ingestion process completed!")
            else:
                print("No votations found to process for September 2025.")

    except Exception as e:
        print(f"A general error occurred: {e}")

if __name__ == "__main__":
    main()