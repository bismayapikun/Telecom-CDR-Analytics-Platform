# Architecture Diagram

## Files

| File | Description |
|------|------------|
| architecture_diagram.png | Architecture diagram image |
| CDR_Architecture_Diagram.drawio | Editable Draw.io source file |

---

## Architecture Overview

The Telecom CDR Analytics Platform follows a modern data engineering architecture designed for scalability, performance, and governance.

### Data Flow Layers

Data flows through **4 main layers**:

1. **Data Source**  
   - Python generates 50,000 synthetic CDR (Call Detail Record) records.
   - Simulates telecom usage data (calls, duration, cost, location, etc.).

2. **Staging Layer**  
   - Raw data is loaded into Snowflake `STG_CDR` table.
   - Minimal transformation is applied.
   - Used for validation and preprocessing.

3. **ETL Pipeline**  
   - Data is transformed using Spark / SQL.
   - Business rules and aggregations are applied.
   - Data is loaded into the Data Warehouse `FACT_CDR` table.

4. **DWH Layer**  
   - Star Schema model implemented.
   - 1 Fact table (`FACT_CDR`)
   - 5 Dimension tables (Customer, Date, Location, Plan, Device)
   - Optimized for analytical queries.

---

## Supporting Components

- **Apache Spark** – Batch and Streaming processing
- **Snowflake Optimization** – Clustering, Time Travel, Semi-Structured JSON
- **Security** – RBAC, Data Masking, Governance Policies
- **Performance Tuning** – Secure Views, Query Optimization
- **Visualization** – Tableau Dashboard (7 Analytical Charts)

---

## Architecture Highlights

- Scalable cloud-based architecture
- Modular ETL design
- Optimized warehouse performance
- Governance and security best practices
- Analytics-ready star schema design
