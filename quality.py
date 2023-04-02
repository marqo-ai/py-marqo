from typing import Any, Dict, List
import random
import json
import requests
import multiprocessing

import marqo
from pprint import pprint
import csv

def write_dictlist_to_csv(dictlist, filename):
    """
    Writes a list of dictionaries to a CSV file.

    Args:
        dictlist (list): A list of dictionaries.
        filename (str): The name of the output CSV file.

    Returns:
        None
    """
    # Extract the keys from the first dictionary in the list
    fieldnames = list(dictlist[0].keys())

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header row
        writer.writeheader()

        # Write each row of data
        for row in dictlist:
            writer.writerow(row)

index_name="image-scoring-index"
# mq = marqo.Client(url='http://localhost:8882')

words = requests.get("https://www.mit.edu/~ecprice/wordlist.10000").text.splitlines()

def get_random_query() -> str:
    """Get a random 5 word query from the vocabulary.
    """
    return " ".join(random.sample(words, k=5))

def search(n: int, rep_w: int = 0, random_weight_score: int = 0) -> List[Dict[str, Any]]:
    index_name="nft-scoring-index"
    mq = marqo.Client(url='http://localhost:8882')

    try:
        results: List[Dict[str, Any]]= mq.index(index_name).search(
            q=get_random_query(),
            reweight_score_param= "reputation",
            reputation_weight_score=rep_w,
            random_weight_score = random_weight_score,
            searchable_attributes=["image"],
            attributes_to_retrieve=["image", "reputation", "word1", "word2"],
            limit=10
        )["hits"]
    except Exception as e:
        print(e)
        return search(n, rep_w=rep_w, random_weight_score=random_weight_score)

    return [
        {
            "rand_w": random_weight_score,
            "rep_w": rep_w,
            "reputation":  r["reputation"],
            "rank": i+1,
            "score": r["_score"],
            "image": r["image"],
            "uniq": f"{r['word1']}_{r['word2']}",
        } for i, r in enumerate(results)
    ]

import functools 

if __name__ == "__main__":
    samples = 100
    output = []
    for rand_w in range(0, 11, 1):
        for repw in range(0, 11, 1):
            with multiprocessing.Pool(20) as p:
                result = p.map(functools.partial(search, rep_w=repw/10, random_weight_score=rand_w/10), range(samples))
                for r in result:
                    output.extend(r)


    json.dump(output, open("result.json", "w"), indent=2)
    write_dictlist_to_csv(output, 'result.csv')



import pandas as pd
df = pd.read_csv("result.csv")
result = []
for rand_w in range(0, 11, 1):
    for repw in range(0, 11, 1):
        t = df[df["rep_w"] ==  repw/10][df["rand_w"] == rand_w/10]
        result.append((
            rand_w/10, repw/10, t["reputation"].corr(t["rank"])
        ))

with open("corr.csv", 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["random", "reputation", "corr_value"])
    csvwriter.writerows(result)
