# LocalMapReduce

A simple local MapReduce simulation using Python and **LMDB** for efficient key-value storage.

## Project Structure

- `data/` — Input text files
- `local_mapreduce.py` — Core MapReduce simulation code
- `requirements.txt` — Project dependencies
- `db/` — LMDB database storage

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Run MapReduce

```bash
python local_mapreduce.py --inputs data/test_input_1.txt data/test_input_2.txt
```

### Clear LMDB Storage

```bash
python local_mapreduce.py --clear
```

## Output Example

```
Map phase completed: Intermediate key-value pairs written to LMDB.

--- Final Word Counts ---
hello: 3
world: 3
there: 2
of: 1
python: 2
is: 1
great: 1
```

## Notes

- This is a local simulation for educational purposes.
- Currently not distributed or parallelized.

---

Built by **Rohan Bali** | [GitHub Profile](https://github.com/rohanbalixz)

