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
        # --- Hidden high-risk accounts (will appear normal until flagged) ---
        {"id": "Victor_Petrov", "type": "Account", "risk": "High"},
        {"id": "Nikolai_Federov", "type": "Account", "risk": "High"},
        
        {"id": "Sarah_Jenkins", "type": "Account", "risk": "High"},
        {"id": "David_Chen", "type": "Account", "risk": "High"},
        {"id": "Michael_Roberts", "type": "Account", "risk": "High"},
        {"id": "Angela_Moretti", "type": "Account", "risk": "High"},
        {"id": "Raj_Patel", "type": "Account", "risk": "High"},
        {"id": "Carlos_Mendes", "type": "Account", "risk": "High"},
        
        {"id": "Apex_Logistics_Ltd", "type": "ShellCompany", "risk": "High"},
        {"id": "Pinnacle_Trading_Co", "type": "ShellCompany", "risk": "High"},
        
        # --- Banks ---
        {"id": "Global_Bank_Inc", "type": "Bank", "risk": "Low"},
        {"id": "Local_Credit_Union", "type": "Bank", "risk": "Low"},
        
        # --- Beneficiaries ---
        {"id": "Greenfield_Exports_LLC", "type": "Beneficiary", "risk": "Low"},
        {"id": "Pacific_Rim_Holdings", "type": "Beneficiary", "risk": "Medium"},
        
        # --- Normal customers/regular people ---
        {"id": "John_Smith", "type": "Account", "risk": "Low"},
        {"id": "Mary_Johnson", "type": "Account", "risk": "Low"},
        {"id": "Robert_Davis", "type": "Account", "risk": "Low"},
        {"id": "Emily_Wilson", "type": "Account", "risk": "Low"},
        {"id": "Michael_Brown", "type": "Account", "risk": "Low"},
        {"id": "Sarah_Taylor", "type": "Account", "risk": "Low"},
        {"id": "David_Miller", "type": "Account", "risk": "Low"},
        {"id": "Jennifer_Garcia", "type": "Account", "risk": "Low"},
        {"id": "James_Martinez", "type": "Account", "risk": "Low"},
        {"id": "Linda_Anderson", "type": "Account", "risk": "Low"},
        {"id": "William_Thomas", "type": "Account", "risk": "Low"},
        {"id": "Patricia_Jackson", "type": "Account", "risk": "Low"},
        {"id": "Joseph_White", "type": "Account", "risk": "Low"},
        {"id": "Margaret_Harris", "type": "Account", "risk": "Low"},
        {"id": "Thomas_Martin", "type": "Account", "risk": "Low"},
        {"id": "Jessica_Thompson", "type": "Account", "risk": "Low"},
        {"id": "Christopher_Garcia", "type": "Account", "risk": "Low"},
        {"id": "Susan_Martinez", "type": "Account", "risk": "Low"},
        {"id": "Daniel_Robinson", "type": "Account", "risk": "Low"},
        {"id": "Karen_Clark", "type": "Account", "risk": "Low"}
    ]
    
    # Relationships to create
    edges = [
        # Placement (high-value initial deposits from hidden criminals)
        ("Victor_Petrov", "Sarah_Jenkins", 150000),
        ("Nikolai_Federov", "Apex_Logistics_Ltd", 80000),
        
        # Layering - First level
        ("Sarah_Jenkins", "David_Chen", 50000),
        ("Sarah_Jenkins", "Michael_Roberts", 50000),
        ("Sarah_Jenkins", "Apex_Logistics_Ltd", 50000),
        
        # Layering - Second level and LOOP 1 (David -> Angela -> Raj -> David)
        ("David_Chen", "Angela_Moretti", 50000),
        ("Angela_Moretti", "Raj_Patel", 40000),
        ("Raj_Patel", "David_Chen", 10000),        # Loop edge
        ("Raj_Patel", "Pinnacle_Trading_Co", 30000),
        ("Angela_Moretti", "Carlos_Mendes", 10000),
        
        # Layering - Shell company complex and LOOP 2 (Apex -> Michael -> Pinnacle -> Apex)
        ("Apex_Logistics_Ltd", "Michael_Roberts", 60000),
        ("Apex_Logistics_Ltd", "Pinnacle_Trading_Co", 70000),
        ("Michael_Roberts", "Pinnacle_Trading_Co", 110000),
        ("Pinnacle_Trading_Co", "Apex_Logistics_Ltd", 20000),  # Loop edge
        
        # Integration
        ("Pinnacle_Trading_Co", "Carlos_Mendes", 190000),
        ("Carlos_Mendes", "Global_Bank_Inc", 200000),
        
        # Final Destination
        ("Global_Bank_Inc", "Greenfield_Exports_LLC", 50000),
        ("Global_Bank_Inc", "Pacific_Rim_Holdings", 150000),
        
        # Extra noise to Local Credit Union
        ("Michael_Roberts", "Local_Credit_Union", 10000),
        ("Local_Credit_Union", "Greenfield_Exports_LLC", 10000),
        
        # Normal customer transactions (legitimate activity)
        ("John_Smith", "Mary_Johnson", 500),
        ("Mary_Johnson", "Robert_Davis", 750),
        ("Robert_Davis", "Emily_Wilson", 300),
        ("Emily_Wilson", "Michael_Brown", 1200),
        ("Michael_Brown", "Sarah_Taylor", 800),
        ("Sarah_Taylor", "David_Miller", 450),
        ("David_Miller", "Jennifer_Garcia", 950),
        ("Jennifer_Garcia", "James_Martinez", 600),
        ("James_Martinez", "Linda_Anderson", 350),
        ("Linda_Anderson", "William_Thomas", 1100),
        ("William_Thomas", "Patricia_Jackson", 700),
        ("Patricia_Jackson", "Joseph_White", 400),
        ("Joseph_White", "Margaret_Harris", 850),
        ("Margaret_Harris", "Thomas_Martin", 550),
        ("Thomas_Martin", "Jessica_Thompson", 900),
        ("Jessica_Thompson", "Christopher_Garcia", 650),
        ("Christopher_Garcia", "Susan_Martinez", 1000),
        ("Susan_Martinez", "Daniel_Robinson", 500),
        ("Daniel_Robinson", "Karen_Clark", 750),
        ("Karen_Clark", "John_Smith", 400),
        
        # Some normal customers also transact with banks
        ("John_Smith", "Global_Bank_Inc", 2000),
        ("Emily_Wilson", "Local_Credit_Union", 1500),
        ("Michael_Brown", "Global_Bank_Inc", 3000),
        ("Sarah_Taylor", "Local_Credit_Union", 1200),
        ("David_Miller", "Global_Bank_Inc", 2500),
        
        # Some normal customers transact with legitimate businesses
        ("William_Thomas", "Greenfield_Exports_LLC", 1800),
        ("Patricia_Jackson", "Greenfield_Exports_LLC", 2200),
        ("Thomas_Martin", "Greenfield_Exports_LLC", 1600)
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
