from typing import Any, Dict, List
import random
import json
import requests

import marqo
from pprint import pprint

index_name="nft-scoring-index"
mq = marqo.Client(url='http://localhost:8882')

words = requests.get("https://www.mit.edu/~ecprice/wordlist.10000").text.splitlines()

def get_random_query() -> str:
    """Get a random 5 word query from the vocabulary.
    """
    return " ".join(random.sample(words, k=5))

def search(rep_w: int = 0, random_weight_score: int = 0) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]]= mq.index(index_name).search(
        q=get_random_query(),
        reweight_score_param= "reputation",
        reputation_weight_score=rep_w,
        random_weight_score = random_weight_score,
        searchable_attributes=["image"],
        attributes_to_retrieve=["image", "reputation", "word1", "word2"],
        limit=10
    )["hits"]

    return [
        {
            "image": r["image"],
            "reputation":  r["reputation"],
            "rank": i+1,
            "score": r["_score"],
            "uniq": f"{r['word1']}_{r['word2']}",
            "rand_w": random_weight_score,
            "rep_w": rep_w,
        } for i, r in enumerate(results)
    ]

samples = 1 # 20
output = []
for rand_w in range(0, 10, 5):
    for rep_w in range(0, 10, 5):
        for i in range(samples):
            output.extend(search(rep_w=rep_w/10, random_weight_score=rand_w/10))

json.dump(output, open("result.json", "w"), indent=2)
