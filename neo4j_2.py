import pandas as pd
from neo4j import GraphDatabase
import logging
import sys
from pyvis.network import Network
from collections import defaultdict

# Setup logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configuration
NEO4J_URI = "bolt://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"
INPUT_CSV = "anomalous_transactions.csv"
ID_MAPPING_CSV = "id_mapping.csv"  # Added ID mapping file
LOAD_BATCH_SIZE = 100000
FAN_THRESHOLD = 2  # Minimum number of connections
MAX_NODES_VISUALIZATION = 3000  # Limit nodes for faster visualization
INTERACTIVE_GRAPH_HTML = "neo4j_graph_interactive.html"

def load_id_mapping(mapping_file):
    """Load ID mapping from CSV file."""
    logging.info("Loading ID mapping from CSV...")
    try:
        mapping_df = pd.read_csv(mapping_file)
        # Create dictionaries for both direction mappings
        mapped_to_original = dict(zip(mapping_df['mapped_id'], mapping_df['original_id']))
        original_to_mapped = dict(zip(mapping_df['original_id'], mapping_df['mapped_id']))
        logging.info(f"Loaded {len(mapped_to_original)} ID mappings")
        return mapped_to_original, original_to_mapped
    except Exception as e:
        logging.error(f"Error loading ID mapping: {str(e)}")
        raise

def create_indexes_and_constraints(driver):
    """Create necessary indexes and constraints for better performance."""
    logging.info("Creating indexes and constraints...")
    with driver.session() as session:
        start_time = pd.Timestamp.now()
        
        # Create indexes
        session.run("CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.id)")
        session.run("CREATE INDEX IF NOT EXISTS FOR ()-[t:TRANSACTION]-() ON (t.amount)")
        
        execution_time = (pd.Timestamp.now() - start_time).total_seconds()
        logging.info(f"Indexes created in {execution_time:.2f} seconds")

def create_batch_transactions(tx, rows, original_to_mapped):
    """Create multiple transactions in a single Cypher query using mapped IDs."""
    params = [{
        'from_id': str(original_to_mapped.get(str(row["From_Account_id"]), str(row["From_Account_id"]))),
        'to_id': str(original_to_mapped.get(str(row["To_Account_id"]), str(row["To_Account_id"]))),
        'amount': float(row["amount"]),
        'dt': row["Date/Time"].replace(" ", "T")
    } for row in rows]
    
    query = """
    UNWIND $params AS param
    MERGE (src:Account {id: param.from_id})
    WITH param, src
    MERGE (dst:Account {id: param.to_id})
    WITH param, src, dst
    CREATE (src)-[:TRANSACTION {
        amount: param.amount,
        datetime: datetime(param.dt)
    }]->(dst)
    """
    
    tx.run(query, params=params)

def load_to_neo4j(driver, csv_path, original_to_mapped, batch_size=10000):
    """Load data to Neo4j in batches using mapped IDs."""
    logging.info("Starting Neo4j data load process...")
    
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    logging.info(f"Total transactions to process: {total_rows:,}")
    
    transaction_count = 0
    start_time = pd.Timestamp.now()
    
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        with driver.session() as session:
            try:
                session.execute_write(create_batch_transactions, batch_df.to_dict('records'), original_to_mapped)
                transaction_count += len(batch_df)
                
                elapsed = (pd.Timestamp.now() - start_time).total_seconds()
                rate = transaction_count / elapsed
                progress = (transaction_count / total_rows) * 100
                logging.info(
                    f"Progress: {progress:.1f}% | "
                    f"Processed: {transaction_count:,}/{total_rows:,} | "
                    f"Rate: {rate:.1f} txn/sec"
                )
                
            except Exception as e:
                logging.error(f"Error processing batch: {str(e)}")
                continue

def analyze_fan_patterns_efficient(driver, fan_threshold):
    """Analyze fan patterns using an optimized approach."""
    logging.info(f"Starting optimized fan pattern analysis with threshold {fan_threshold}")
    
    query = """
    CALL {
        MATCH (a:Account)
        OPTIONAL MATCH (a)<-[in:TRANSACTION]-()
        WITH a, COUNT(in) as fan_in
        OPTIONAL MATCH (a)-[out:TRANSACTION]->()
        WITH a, fan_in, COUNT(out) as fan_out
        WHERE fan_in >= $threshold OR fan_out >= $threshold
        RETURN 
            a.id as account_id,
            fan_in,
            fan_out,
            CASE 
                WHEN fan_in >= $threshold AND fan_out >= $threshold THEN 'BOTH'
                WHEN fan_in >= $threshold THEN 'FAN_IN'
                ELSE 'FAN_OUT'
            END as pattern_type,
            fan_in + fan_out as total_connections
    }
    RETURN *
    ORDER BY total_connections DESC
    """
    
    results = []
    with driver.session() as session:
        try:
            results = session.run(query, threshold=fan_threshold).data()
            logging.info(f"Found {len(results)} accounts with fan patterns")
            
            # Log pattern distribution
            pattern_counts = {'BOTH': 0, 'FAN_IN': 0, 'FAN_OUT': 0}
            for r in results:
                pattern_counts[r['pattern_type']] += 1
            
            logging.info("Pattern distribution:")
            for pattern, count in pattern_counts.items():
                logging.info(f"  {pattern}: {count}")
                
        except Exception as e:
            logging.error(f"Error in fan pattern analysis: {str(e)}")
    
    return results

def visualize_graph_interactive_efficient(driver, patterns, mapped_to_original, output_path):
    """Create memory-efficient interactive graph visualization with arrows using original IDs."""
    logging.info("Starting memory-efficient visualization generation...")
    
    # Initialize network
    net = Network(height="800px", width="100%", directed=True)
    
    # Convert patterns to use original IDs
    pattern_accounts = {
        mapped_to_original.get(p['account_id'], p['account_id']): p 
        for p in patterns
    }
    
    try:
        # Get transactions in batches
        edges = get_paginated_transactions(driver, pattern_accounts, mapped_to_original)
        
        # Collect unique nodes
        nodes = set()
        for edge in edges:
            nodes.add(edge['source'])
            nodes.add(edge['target'])
        
        logging.info(f"Total unique nodes before optimization: {len(nodes)}")
        
        # Add nodes with enhanced information
        for node in nodes:
            try:
                color = 'skyblue'
                size = 25
                title_parts = [f"ID: {node}"]
                
                if node in pattern_accounts:
                    pattern = pattern_accounts[node]
                    pattern_type = pattern['pattern_type']
                    if pattern_type == 'BOTH':
                        color = '#ff4444'
                    elif pattern_type == 'FAN_IN':
                        color = '#ffaa00'
                    else:  # FAN_OUT
                        color = '#ff7700'
                    size = 30
                    title_parts.extend([
                        f"Pattern: {pattern_type}",
                        f"Fan-in: {pattern['fan_in']}",
                        f"Fan-out: {pattern['fan_out']}",
                        f"Total Connections: {pattern['total_connections']}"
                    ])
                
                net.add_node(str(node),
                            label=str(node),
                            title="\n".join(title_parts),
                            color=color,
                            size=size)
                            
            except Exception as e:
                logging.warning(f"Error adding node {node}: {str(e)}")
                continue
        
        # Add edges with arrows
        for edge in edges:
            try:
                net.add_edge(
                    str(edge['source']),
                    str(edge['target']),
                    title=f"Amount: ${edge['amount']:,.2f}\nDate: {edge['datetime']}",
                    color='#000000',
                    width=1,
                    arrows={'to': {'enabled': True, 'type': 'arrow'}}
                )
            except Exception as e:
                logging.warning(f"Error adding edge {edge}: {str(e)}")
                continue
        
        # Add legend
        legend_y = -100
        for label, color in [
            ("Normal Account", "skyblue"),
            ("Fan-in Pattern", "#ffaa00"),
            ("Fan-out Pattern", "#ff7700"),
            ("Both Patterns", "#ff4444")
        ]:
            try:
                net.add_node(f"legend_{label}",
                            label=label,
                            color=color,
                            x=100,
                            y=legend_y)
                legend_y += 50
            except Exception as e:
                logging.warning(f"Error adding legend node: {str(e)}")
                continue
        
        logging.info("Saving visualization...")
        net.save_graph(output_path)
        logging.info(f"Visualization saved to {output_path}")
        
    except Exception as e:
        logging.error(f"Error generating visualization: {str(e)}")
        raise

def get_paginated_transactions(driver, pattern_accounts, mapped_to_original, batch_size=1000):
    """Retrieve transactions in batches to avoid memory issues."""
    logging.info("Starting paginated transaction retrieval...")
    
    all_edges = []
    processed = 0
    
    # Convert pattern_accounts keys to mapped IDs for the query
    mapped_pattern_ids = [mapped_to_original.get(id, id) for id in pattern_accounts.keys()]
    
    query = """
    MATCH (src:Account)-[t:TRANSACTION]->(dst:Account)
    WHERE src.id IN $pattern_ids OR dst.id IN $pattern_ids
    WITH src, dst, t
    ORDER BY t.amount DESC
    SKIP $skip
    LIMIT $batch_size
    RETURN DISTINCT 
        src.id as source, 
        dst.id as target, 
        t.amount as amount,
        t.datetime as datetime
    """
    
    with driver.session() as session:
        while True:
            try:
                logging.info(f"Fetching batch starting at offset {processed}")
                result = session.run(
                    query, 
                    pattern_ids=mapped_pattern_ids,
                    skip=processed,
                    batch_size=batch_size
                ).data()
                
                if not result:
                    break
                
                # Convert IDs back to original IDs
                for edge in result:
                    edge['source'] = mapped_to_original.get(edge['source'], edge['source'])
                    edge['target'] = mapped_to_original.get(edge['target'], edge['target'])
                
                all_edges.extend(result)
                processed += len(result)
                logging.info(f"Retrieved {processed} transactions so far")
                
                if len(result) < batch_size:
                    break
                    
            except Exception as e:
                logging.error(f"Error retrieving batch at offset {processed}: {str(e)}")
                break
    
    logging.info(f"Total transactions retrieved: {len(all_edges)}")
    return all_edges

def main():
    """Main function to run the complete analysis."""
    
    try:
        # Load ID mapping
        mapped_to_original, original_to_mapped = load_id_mapping(ID_MAPPING_CSV)
        
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Step 1: Load data and create indexes
        load_to_neo4j(driver, INPUT_CSV, original_to_mapped, LOAD_BATCH_SIZE)
        create_indexes_and_constraints(driver)
        
        # Step 2: Analyze patterns
        patterns = analyze_fan_patterns_efficient(driver, FAN_THRESHOLD)
        
        # Convert pattern results to use original IDs for display
        for pattern in patterns:
            pattern['account_id'] = mapped_to_original.get(pattern['account_id'], pattern['account_id'])
        
        # Print pattern details
        pattern_types = {'BOTH': [], 'FAN_IN': [], 'FAN_OUT': []}
        for account in patterns:
            pattern_types[account['pattern_type']].append(account)
        
        print("\nPattern Analysis:")
        for pattern_type, accounts in pattern_types.items():
            print(f"\n{pattern_type} Patterns ({len(accounts)} accounts):")
            for account in sorted(accounts[:5], key=lambda x: x['total_connections'], reverse=True):
                print(f"  Account {account['account_id']}: "
                      f"fan-in={account['fan_in']}, "
                      f"fan-out={account['fan_out']}, "
                      f"total connections={account['total_connections']}")
            if len(accounts) > 5:
                print(f"  ... and {len(accounts)-5} more")
        
        # Create visualization
        print("\nGenerating interactive visualization...")
        visualize_graph_interactive_efficient(driver, patterns, mapped_to_original, INTERACTIVE_GRAPH_HTML)
        print(f"\nVisualization saved to {output_path}")
        
    except Exception as e:
        logging.error(f"Error in main analysis: {str(e)}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)