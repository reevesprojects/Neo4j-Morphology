import pandas as pd
import time
import math
from collections import defaultdict
import neo4j_utils

# ==========================================
# CONFIGURATION
# ==========================================
BATCH_SIZE = 2000

QUERY_CREATE_NODES = """
UNWIND $batch AS item

// 1. Create Lexeme Node
MERGE (l:Lexeme {id: item.id})
SET l.lemma = item.lemma,
    l.pos = item.pos,
    l.corpus_count = item.corpus_count,
    l.corpus_log_count = item.corpus_log_count,
    l.lang = item.lang,
    l.features = item.features,
    l.misc = item.misc

// 2. Process Morphs
FOREACH (char_data IN item.chars |
    MERGE (m:Morph {id: char_data.id})
    // Use the pre-calculated sums from the batch
    SET m.text = char_data.text,
        m.type = "character",
        m.lang = item.lang,
        m.corpus_count = char_data.corpus_count,
        m.corpus_log_count = char_data.corpus_log_count

    // Link Morph -> Lexeme
    MERGE (m)-[r:COMPONENT]->(l)
    SET r.order = char_data.order,
        r.type = "Compounding"
)
"""

def load_and_prep_data():
    print("Loading InterCorp data...")
    try:
        inter_df = pd.read_csv('InterCorp_v16ud_100k.csv', sep=';', on_bad_lines='skip')
    except FileNotFoundError:
        print("Error: InterCorp_v16ud_100k.csv not found.")
        return [], {}, {}

    # Clean columns and data
    inter_df.columns = [c.strip().replace('"', '') for c in inter_df.columns]
    inter_df['freq'] = pd.to_numeric(inter_df['freq'], errors='coerce').fillna(0)

    # Get highest frequency entry for each word
    inter_df = inter_df.sort_values('freq', ascending=False).drop_duplicates(subset=['word'])
    stats_map = inter_df.set_index('word')[['upos', 'freq']].to_dict('index')

    print("Loading Corpus words...")
    try:
        with open('any_corp_AND_any_dict.tsv', 'r', encoding='utf-8') as f:
            corpus_words = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Error: any_corp_AND_any_dict.tsv not found.")
        return [], {}, {}

    # --- PRE-CALCULATION ---
    # Sum the frequency of all words connected to a character
    print("Calculating character frequencies...")
    char_freq_map = defaultdict(float)

    for word in corpus_words:
        # Get frequency of the word (default to 0 if not found)
        w_freq = stats_map.get(word, {}).get('freq', 0)

        # Add word's frequency to each of its UNIQUE characters
        for char in set(word):
            char_freq_map[char] += w_freq

    return corpus_words, stats_map, char_freq_map

def prepare_batch_data(words, stats_map, char_freq_map):
    prepared_data = []
    total = len(words)
    start_time = time.time()
    print(f"Preparing data for {total} words...")

    for i, word in enumerate(words):
        # 1. Word Stats
        w_stats = stats_map.get(word, {'upos': 'UNKNOWN', 'freq': 0})
        w_freq = float(w_stats['freq'])
        w_log = math.log(w_freq + 1)

        # 2. Morph (Character) Data
        chars_data = []
        for idx, char in enumerate(word):
            # Lookup the pre-summed frequency for this character
            c_freq = char_freq_map.get(char, 0)
            c_log = math.log(c_freq + 1)

            chars_data.append({
                'id': char,
                'text': char,
                'order': idx,
                'corpus_count': c_freq,
                'corpus_log_count': c_log
            })

        # 3. Add to List
        prepared_data.append({
            'id': word,
            'lemma': word,
            'pos': w_stats['upos'],
            'lang': 'zh',
            'corpus_count': w_freq,
            'corpus_log_count': w_log,
            'features': "{}",
            'misc': "{}",
            'chars': chars_data
        })

        if (i + 1) % 10000 == 0:
             print(f"Processed {i + 1}/{total} words... ({time.time() - start_time:.2f}s)")
    
    return prepared_data

def main():
    words, stats, char_stats = load_and_prep_data()
    
    if not words:
        print("Skipping graph construction due to missing data.")
        return

    try:
        driver = neo4j_utils.get_driver()
        
        # Prepare data in memory
        batch_data = prepare_batch_data(words, stats, char_stats)
        
        # Create Constraints (if any, though not strictly defined in original script yet)
        # neo4j_utils.create_constraints(driver, ["CREATE CONSTRAINT FOR (l:Lexeme) REQUIRE l.id IS UNIQUE"])

        # Insert Data
        neo4j_utils.batch_insert(driver, QUERY_CREATE_NODES, batch_data, batch_size=BATCH_SIZE, batch_param_name="batch")

        print("Graph construction complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main()
