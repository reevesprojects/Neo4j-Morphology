import os
import json
import math
import derinet.lexicon as dlex
import neo4j_utils

# --- Configuration ---
DERINET_DATA_PATH = "derinet-2-3.tsv"
BATCH_SIZE = 5000

# NODE MERGE QUERY (Lexemes + Morph Segmentation)
NODE_MERGE_QUERY = """
UNWIND $batch AS word_data
MERGE (l:Lexeme {id: word_data.id})
ON CREATE SET
    l.lemma = word_data.lemma,
    l.pos = word_data.pos,
    l.lang = word_data.lang,
    l.is_root = word_data.is_root,
    l.features = word_data.features,
    l.misc = word_data.misc,
    l.corpus_stats = word_data.corpus_stats,
    l.corpus_count = word_data.corpus_count

FOREACH (seg IN word_data.segmentation_list |
    MERGE (m:Morph {id: seg.morph})
    ON CREATE SET 
        m.text = seg.morph, 
        m.lang = word_data.lang,
        m.type = "segment"
    MERGE (m)-[r:COMPONENT]->(l)
    SET r.order = seg.order,
        r.type = "Composition"
)
"""

# REL MERGE QUERY (Lexeme -> Lexeme Derivation)
REL_MERGE_QUERY = """
UNWIND $batch AS rel_data
MATCH (parent:Lexeme {id: rel_data.parent_id})
MATCH (child:Lexeme {id: rel_data.child_id})
MERGE (parent)-[r:COMPONENT {type: rel_data.type}]->(child)
"""

def prepare_data(lexicon):
    word_nodes = []
    word_relations = []

    print("Iterating over lexicon to prepare data...")

    for lexeme in lexicon.iter_lexemes():
        
        primary_parent_id = lexeme.parent.lemid if lexeme.parent else None

        # Parse JSON stats
        corpus_stats_str = "{}"
        absolute_count = 0
        if hasattr(lexeme, 'extra_data') and lexeme.extra_data:
             if 'corpus_stats' in lexeme.extra_data:
                stats = lexeme.extra_data['corpus_stats']
                corpus_stats_str = json.dumps(stats)
                absolute_count = stats.get('absolute_count', 0)

        # Handle Segmentation
        segmentation_list = []
        if hasattr(lexeme, 'segmentation') and lexeme.segmentation:
            for idx, seg in enumerate(lexeme.segmentation):
                segmentation_list.append({
                    'morph': seg,
                    'order': idx
                })
        
        # Original segmentation string for property
        morph_json = json.dumps(lexeme.segmentation) if hasattr(lexeme, 'segmentation') else "[]"

        word_nodes.append({
            "id": lexeme.lemid,
            "lemma": lexeme.lemma,
            "pos": str(lexeme.pos),
            "lang": getattr(lexeme, 'lang', 'cs'),
            "is_root": primary_parent_id is None,
            "features": json.dumps(lexeme.feats),
            "misc": json.dumps(lexeme.misc),
            "morphology": morph_json,
            "corpus_stats": corpus_stats_str,
            "corpus_count": absolute_count,
            "corpus_log_count": math.log(absolute_count + 1),
            "segmentation_list": segmentation_list 
        })

        if hasattr(lexeme, 'parent_relations'):
            for rel in lexeme.parent_relations:
                rel_type = getattr(rel, 'type', 'Derivation')

                for source_lexeme in rel.sources:
                    word_relations.append({
                        "child_id": lexeme.lemid,
                        "parent_id": source_lexeme.lemid,
                        "type": rel_type
                    })

    return word_nodes, word_relations

def main():
    print(f"Loading DeriNet data from {DERINET_DATA_PATH}...")
    lexicon = None
    try:
        lexicon = dlex.Lexicon()
        lexicon.load(data_source=DERINET_DATA_PATH, fmt=dlex.Format.DERINET_V2)
        print("✅ DeriNet file loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load DeriNet data: {e}")
        return

    word_nodes, word_relations = prepare_data(lexicon)

    num_words = len(word_nodes)
    if num_words == 0:
        print("❌ Error: Lexicon yielded 0 nodes.")
        return

    # --- Filter Word Nodes by Frequency ---
    print(f"Filtering word nodes by corpus_absolute_count. Total nodes: {len(word_nodes)}")
    word_nodes.sort(key=lambda x: x['corpus_count'], reverse=True)
    word_nodes = word_nodes[:75000]
    top_75k_word_ids = {node['id'] for node in word_nodes}

    # Filter word_relations
    word_relations = [rel for rel in word_relations if rel['parent_id'] in top_75k_word_ids and rel['child_id'] in top_75k_word_ids]

    print(f"✅ Filtered nodes to top 75,000. Remaining: {len(word_nodes)}")
    print(f"✅ Filtered relationships. Remaining: {len(word_relations)}")

    try:
        driver = neo4j_utils.get_driver()

        # Create constraint
        neo4j_utils.create_constraints(driver, [
            "CREATE CONSTRAINT word_id_unique IF NOT EXISTS FOR (l:Lexeme) REQUIRE l.id IS UNIQUE;"
        ])

        # Insert Lexemes (and Morphs)
        neo4j_utils.batch_insert(driver, NODE_MERGE_QUERY, word_nodes, batch_size=BATCH_SIZE, batch_param_name="batch")

        # Insert Relationships (Lexeme -> Lexeme)
        neo4j_utils.batch_insert(driver, REL_MERGE_QUERY, word_relations, batch_size=BATCH_SIZE, batch_param_name="batch")

        print("\n✨ Data import complete.")

    except Exception as e:
        print(f"\n❌ A database error occurred: {e}")
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main()
