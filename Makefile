# Makefile for Neo4j Morphology Project

PYTHON = python
PIP = pip

.PHONY: install download-derinet run-chinese run-derinet clean

install:
	$(PIP) install -r requirements.txt

download-derinet:
	@echo "Downloading DeriNet 2.3 data package..."
	curl -o derinet-2-3-all.zip "https://lindat.mff.cuni.cz/repository/server/api/core/items/62540779-b206-4cf7-ac33-399ce68e35e6/allzip?handleId=11234/1-5846"
	@echo "Extracting DeriNet data..."
	unzip -o derinet-2-3-all.zip
	@echo "Cleanup zip file..."
	rm derinet-2-3-all.zip
	@echo "DeriNet data ready."

run-chinese:
	@echo "Running Chinese to Neo4j conversion..."
	$(PYTHON) chinese_to_neo4j.py

run-derinet:
	@echo "Running DeriNet to Neo4j conversion..."
	@if [ ! -f derinet-2-3.tsv ]; then \
		echo "derinet-2-3.tsv not found. Running download-derinet first..."; \
		$(MAKE) download-derinet; \
	fi
	$(PYTHON) derinet_to_neo4j.py

clean:
	@echo "Removing downloaded data..."
	rm -f derinet-2-3-all.zip derinet-2-3.tsv
	@echo "Clean complete."
