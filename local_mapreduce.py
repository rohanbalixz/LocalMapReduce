# local_mapreduce.py
import plyvel
import os
import re
import argparse

DB_PATH = 'db/leveldb_mapreduce'

class LocalMapReduce:
    def __init__(self, db_path=DB_PATH):
        self.db = plyvel.DB(db_path, create_if_missing=True)

    def map_phase(self, input_files):
        word_pattern = re.compile(r'\w+')

        with self.db.write_batch() as b:
            for file_path in input_files:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        words = word_pattern.findall(line.lower())
                        for word in words:
                            key = word.encode('utf-8')
                            existing = self.db.get(key)
                            count = int(existing.decode('utf-8')) + 1 if existing else 1
                            b.put(key, str(count).encode('utf-8'))

        print("Map phase completed: Intermediate key-value pairs written to LevelDB.")

    def reduce_phase(self):
        print("\n--- Final Word Counts ---")
        for key, value in self.db:
            print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")

    def clear_db(self):
        self.db.close()
        plyvel.destroy_db(DB_PATH)
        print("LevelDB database cleared.")

    def close_db(self):
        self.db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simulated Local MapReduce with LevelDB')
    parser.add_argument('--inputs', nargs='+', help='List of input text files', required=False)
    parser.add_argument('--clear', action='store_true', help='Clear the existing LevelDB store')

    args = parser.parse_args()
    lmr = LocalMapReduce()

    if args.clear:
        lmr.clear_db()
    elif args.inputs:
        lmr.map_phase(args.inputs)
        lmr.reduce_phase()
        lmr.close_db()
    else:
        print("Please provide --inputs <files> or --clear.")

