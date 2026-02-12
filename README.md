# Neo4j Morphology Project

This project imports morphological data into a Neo4j database. It supports importing Chinese morphological data and Czech DeriNet data.

## Prerequisites

- Python 3.x
- Neo4j Database (running and accessible)

## Setup

1.  **Install Dependencies**:
    Run the following command to install the required Python packages:
    ```bash
    make install
    ```
    Or manually:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuration**:
    Create a `.env` file in the root directory to store your Neo4j credentials. You can use the provided example as a template:
    ```bash
    cp .env.example .env
    ```
    
    Edit `.env` and fill in your details:
    ```ini
    NEO4J_URI=bolt://localhost:7687
    NEO4J_USERNAME=neo4j
    NEO4J_PASSWORD=your_password
    ```

## Data Preparation

### Chinese Data
The Chinese import script (`chinese_to_neo4j.py`) requires specific data files. **You must provide these files manually** and place them in the root directory of the project:

- `InterCorp_v16ud_100k.csv`: Source file for Chinese corpus data.
- `any_corp_AND_any_dict.tsv`: Source file for corpus words.

### DeriNet Data (Czech)
The DeriNet import script (`derinet_to_neo4j.py`) uses `derinet-2-3.tsv`. You can download this automatically using the Makefile:

```bash
make download-derinet
```

## Running the Import

The project uses a `Makefile` to simplify running the scripts.

### Import Chinese Data
To run the Chinese morphology import:
```bash
make run-chinese
```

### Import DeriNet Data
To run the DeriNet (Czech) morphology import:
```bash
make run-derinet
```
*Note: This command will automatically check for and download the DeriNet data if it is missing.*

### Clean Up
To remove downloaded DeriNet data files:
```bash
make clean
```
