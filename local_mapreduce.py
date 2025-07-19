import lmdb
import os
import re
import argparse

DB_PATH = 'db/lmdb_mapreduce'

class LocalMapReduce:
    def __init__(self, db_path=DB_PATH):
        os.makedirs(db_path, exist_ok=True)
        self.env = lmdb.open(db_path, map_size=10485760)  # 10MB

    def map_phase(self, input_files):
        word_pattern = re.compile(r'\w+')
        with self.env.begin(write=True) as txn:
            for file_path in input_files:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        words = word_pattern.findall(line.lower())
                        for word in words:
                            key = word.encode('utf-8')
                            value = txn.get(key)
                            count = int(value.decode('utf-8')) + 1 if value else 1
                            txn.put(key, str(count).encode('utf-8'))
        print("Map phase completed: Intermediate key-value pairs written to LMDB.")

    def reduce_phase(self):
        print("\n--- Final Word Counts ---")
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")

    def clear_db(self):
        self.env.close()
        for filename in os.listdir(DB_PATH):
            file_path = os.path.join(DB_PATH, filename)
            os.remove(file_path)
        print("LMDB database cleared.")

    def close_db(self):
        self.env.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simulated Local MapReduce with LMDB')
    parser.add_argument('--inputs', nargs='+', help='List of input text files', required=False)
    parser.add_argument('--clear', action='store_true', help='Clear the existing LMDB store')

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
