import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_driver():
    """
    Establishes and returns a Neo4j driver instance.
    """
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not password:
        raise ValueError("Please set NEO4J_URI and NEO4J_PASSWORD in .env file")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"✅ Connection to Neo4j established at {uri}")
    return driver

def create_constraints(driver, constraints):
    """
    Creates unique constraints/indices in the database.
    :param driver: Neo4j driver instance
    :param constraints: List of Cypher query strings to create constraints
    """
    with driver.session() as session:
        for query in constraints:
            try:
                session.run(query)
                print(f"✅ Constraint created: {query.strip()}")
            except Exception as e:
                print(f"⚠️  Error creating constraint: {e}")

def batch_insert(driver, query, data_list, batch_size=5000, batch_param_name="batch"):
    """
    Generic function to insert data in batches.
    :param driver: Neo4j driver instance
    :param query: Cypher query expecting a list parameter
    :param data_list: List of dictionaries to insert
    :param batch_size: Number of records per batch
    :param batch_param_name: The name of the parameter in the Cypher query (default: 'batch')
    """
    item_name = batch_param_name.capitalize()
    print(f"\nStarting {item_name} insertion...")
    total_count = len(data_list)

    if total_count == 0:
        print(f"No {item_name} to insert.")
        return

    # Helper function to run transaction
    def run_tx(tx, batch_data):
        tx.run(query, {batch_param_name: batch_data})

    for i in range(0, total_count, batch_size):
        batch = data_list[i:i + batch_size]
        try:
            with driver.session() as session:
                session.execute_write(run_tx, batch)
            print(f"-> Batch {i//batch_size + 1} of {total_count//batch_size + 1} processed ({len(batch)} items).")
        except Exception as e:
            print(f"!!! Error in {item_name} batch starting at index {i}. Stopping: {e}")
            raise
