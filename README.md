# KORE-Knowledge-Orchestration-and-Retrieval-Engine

kore-system/
├── docker-compose.yml          # (You have this)
├── .env                        # API Keys (Gemini, Neo4j, etc.)
├── requirements.txt
├── data/
│   ├── raw_events.json         # The seed file with mocked events
│   └── policies/               # PDF/Text files for company policies
├── src/
│   ├── config.py               # Central config (Topic names, DB Creds)
│   ├── simulation/
│   │   ├── mock_data.py        # Script to generate random JSON events
│   │   └── producer.py         # Reads JSON -> Pushes to Kafka
│   ├── ingestion/
│   │   ├── consumer_graph.py   # Kafka -> Neo4j (Structure)
│   │   ├── consumer_vector.py  # Kafka -> ChromaDB (Semantic)
│   │   └── utils.py            # Text cleaning, embedding wrappers
│   ├── query_engine/
│   │   ├── tools.py            # Custom Tools: GraphSearch, VectorSearch
│   │   ├── agents.py           # CrewAI/LangGraph Agent definitions
│   │   └── citation_manager.py # Formats "evidence" from metadata
│   └── ui/
│       └── app.py              # Streamlit Interface
└── notebooks/                  # For testing individual components