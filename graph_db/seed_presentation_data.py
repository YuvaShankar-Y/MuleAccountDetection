import os
import logging
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def seed_database():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    # Cypher query to clear the existing database
    clear_query = "MATCH (n) DETACH DELETE n"
    
    # Nodes to create
    nodes = [
        {"id": "Criminal_Syndicate_X", "type": "Criminal", "risk": "High"},
        {"id": "DarkWeb_Vendor_Y", "type": "Criminal", "risk": "High"},
        
        {"id": "Mule_Account_01", "type": "Mule", "risk": "High"},
        {"id": "Mule_Account_02", "type": "Mule", "risk": "High"},
        {"id": "Mule_Account_03", "type": "Mule", "risk": "High"},
        {"id": "Mule_Account_04", "type": "Mule", "risk": "High"},
        {"id": "Mule_Account_05", "type": "Mule", "risk": "High"},
        {"id": "Mule_Account_06", "type": "Mule", "risk": "High"},
        
        {"id": "Shell_Company_Alpha", "type": "ShellCompany", "risk": "High"},
        {"id": "Shell_Company_Beta", "type": "ShellCompany", "risk": "High"},
        
        {"id": "Global_Bank_Inc", "type": "Bank", "risk": "Low"},
        {"id": "Local_Credit_Union", "type": "Bank", "risk": "Low"},
        
        {"id": "Legitimate_Business_LLC", "type": "Beneficiary", "risk": "Low"},
        {"id": "Offshore_Holding_Corp", "type": "Beneficiary", "risk": "Medium"}
    ]
    
    # Relationships to create
    edges = [
        # Placement
        ("Criminal_Syndicate_X", "Mule_Account_01", 150000),
        ("DarkWeb_Vendor_Y", "Shell_Company_Alpha", 80000),
        
        # Layering - First level
        ("Mule_Account_01", "Mule_Account_02", 50000),
        ("Mule_Account_01", "Mule_Account_03", 50000),
        ("Mule_Account_01", "Shell_Company_Alpha", 50000),
        
        # Layering - Second level and LOOP 1 (Mule_02 -> Mule_04 -> Mule_05 -> Mule_02)
        ("Mule_Account_02", "Mule_Account_04", 50000),
        ("Mule_Account_04", "Mule_Account_05", 40000),
        ("Mule_Account_05", "Mule_Account_02", 10000), # Loop edge
        ("Mule_Account_05", "Shell_Company_Beta", 30000),
        ("Mule_Account_04", "Mule_Account_06", 10000),
        
        # Layering - Shell company complex and LOOP 2 (Shell_Alpha -> Mule_03 -> Shell_Beta -> Shell_Alpha)
        ("Shell_Company_Alpha", "Mule_Account_03", 60000),
        ("Shell_Company_Alpha", "Shell_Company_Beta", 70000),
        ("Mule_Account_03", "Shell_Company_Beta", 110000),
        ("Shell_Company_Beta", "Shell_Company_Alpha", 20000), # Loop edge
        
        # Integration
        ("Shell_Company_Beta", "Mule_Account_06", 190000),
        ("Mule_Account_06", "Global_Bank_Inc", 200000),
        
        # Final Destination
        ("Global_Bank_Inc", "Legitimate_Business_LLC", 50000),
        ("Global_Bank_Inc", "Offshore_Holding_Corp", 150000),
        
        # Extra noise to Local Credit Union
        ("Mule_Account_03", "Local_Credit_Union", 10000),
        ("Local_Credit_Union", "Legitimate_Business_LLC", 10000)
    ]

    try:
        with driver.session() as session:
            logger.info("Clearing existing database...")
            session.run(clear_query)
            
            logger.info("Seeding nodes...")
            for node in nodes:
                session.run(
                    "CREATE (a:Account {account_id: $id, entity_type: $type, risk_profile: $risk})",
                    id=node["id"], type=node["type"], risk=node["risk"]
                )
                
            logger.info("Seeding relationships...")
            for edge in edges:
                session.run(
                    """
                    MATCH (a:Account {account_id: $src}), (b:Account {account_id: $dst})
                    CREATE (a)-[:TRANSFER {amount: $amt}]->(b)
                    """,
                    src=edge[0], dst=edge[1], amt=edge[2]
                )
                
        logger.info("Successfully seeded presentation dataset!")
    except Exception as e:
        logger.error(f"Error seeding database: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    seed_database()
