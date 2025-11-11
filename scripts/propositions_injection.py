import os
import time
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("DATABASE")
password = os.getenv("PASSWORD")

NEO4J_URI = os.getenv("URI")
NEO4J_AUTH = (database_url, password)

def flatten_nested_objects(props):
    if 'statusProposicao' in props and isinstance(props['statusProposicao'], dict):
        status_props = props.pop('statusProposicao')
        for key, value in status_props.items():
            if value is not None:
                props[f'status_{key}'] = value

    return {k: v for k, v in props.items() if not isinstance(v, (dict, list))}

def fetch_api_data(url):
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            print(f"  - Warning: API returned 404 Not Found for URL: {url}")
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  - Error fetching from API: {e}")
        return None

def get_votations_to_process(driver):
    print("Fetching votations from the database that have a linked proposition...")
    query = """
    MATCH (v:Votation)
    WHERE v.uriProposicaoObjeto IS NOT NULL
    RETURN v.id AS votationId, v.uriProposicaoObjeto AS propositionUri
    """
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

def link_votation_to_proposition(driver, votation_id, proposition_props):
    print(f"  - Linking Votation '{votation_id}' to Proposition '{proposition_props['id']}'...")
    query = """
    MATCH (v:Votation {id: $votation_id})
    MERGE (p:Proposition {id: $proposition_props.id})
    SET p += $proposition_props
    MERGE (v)-[:REFERS_TO]->(p)
    """
    with driver.session() as session:
        session.run(query, votation_id=votation_id, proposition_props=proposition_props)

def link_authors_to_proposition(driver, proposition_id, authors_data):
    if not authors_data:
        print("  - No authors found to link.")
        return

    print(f"  - Linking {len(authors_data)} authors to Proposition '{proposition_id}'...")
    query = """
    MATCH (p:Proposition {id: $proposition_id})
    UNWIND $authors AS author_data
    WITH p, author_data, split(author_data.uri, '/')[-1] AS deputy_id
    MATCH (d:Deputy {id: toInteger(deputy_id)})
    MERGE (d)-[:AUTHOR_OF]->(p)
    """
    with driver.session() as session:
        session.run(query, proposition_id=proposition_id, authors=authors_data)

def main():
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH) as driver:
            driver.verify_connectivity()
            print("Successfully connected to Neo4j.")
            
            votations_to_process = get_votations_to_process(driver)
            total = len(votations_to_process)
            print(f"Found {total} votations with proposition links to process.")
            
            for i, votation in enumerate(votations_to_process):
                votation_id = votation['votationId']
                proposition_uri = votation['propositionUri']
                
                print(f"\nProcessing Votation {i+1}/{total} (ID: {votation_id})...")
                
                proposition_data = fetch_api_data(proposition_uri)
                
                if proposition_data and "dados" in proposition_data:
                    
                    proposition_props_raw = proposition_data["dados"]
                    proposition_props_clean = flatten_nested_objects(proposition_props_raw)
                    
                    proposition_id = proposition_props_clean['id']
                    
                    link_votation_to_proposition(driver, votation_id, proposition_props_clean)
                    
                    authors_uri = proposition_props_raw.get('uriAutores') 
                    if authors_uri:
                        authors_response = fetch_api_data(authors_uri)
                        if authors_response and "dados" in authors_response:
                            authors_list = authors_response["dados"]
                            link_authors_to_proposition(driver, proposition_id, authors_list)
                    else:
                        print("  - This proposition has no authors URI.")
                
                time.sleep(0.5)

            print("\nProcess completed successfully!")

    except Exception as e:
        print(f"A general error occurred: {e}")

if __name__ == "__main__":
    main()