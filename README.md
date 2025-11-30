# KORE-Knowledge-Orchestration-and-Retrieval-Engine

┌─────────────────────────────────────────────────────────────┐
│                     EVENT SOURCES                            │
│  GitHub (PRs) │ Jira (Tickets) │ Git (Commits) │ Slack      │
└────────┬────────────────┬───────────────┬──────────┬─────────┘
         │                │               │          │
         └────────────────┴───────────────┴──────────┘
                          │
                    Kafka Topics
         ┌────────────────┴────────────────┐
         │  raw-git-prs │ raw-jira-tickets │
         │  raw-git-commits │ raw-slack-chats │
         └────────────────┬────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Kafka Consumer       │
              │  (kafka_consumer.py)  │
              │                       │
              │  • State Management   │
              │  • Selective Indexing │
              │  • Lifecycle Tracking │
              └───┬───────────────┬───┘
                  │               │
         ┌────────▼────┐    ┌────▼──────┐
         │   Neo4j     │    │ ChromaDB  │
         │   (Graph)   │    │ (Vectors) │
         │             │    │           │
         │ • WHO owns? │    │ • Policies│
         │ • Relations │    │ • Events  │
         │ • State     │    │ • Search  │
         └─────────────┘    └───────────┘
                  │               │
                  └───────┬───────┘
                          │
         ┌────────────────▼────────────────┐
         │         AGENT LAYER             │
         │                                 │
         │  Interactive        Autonomous  │
         │  ┌──────────┐      ┌──────────┐│
         │  │Researcher│      │Sentinel  ││
         │  │Writer    │      │Commander ││
         │  └──────────┘      └──────────┘│
         │                                 │
         │  Tools (tools.py):              │
         │  • Expert Finder (WHO)          │
         │  • Document Search (WHAT)       │
         │  • Recent Activity (WHEN)       │
         │  • State Checkers               │
         └─────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────┐
              │   Streamlit UI    │
              │   (app.py)        │
              └───────────────────┘



MAKE SURE ANY JIRA IS RELATED TO SOME REPO/PR AND OTHER WAY AROUND