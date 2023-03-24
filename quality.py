from typing import Any, Dict, List
import random

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

def search(use_rep_score: bool = True, random_weight_score: int = 0) -> List[Dict[str, Any]]:
    reweight_score_param = "reputation" if use_rep_score else None
    results: List[Dict[str, Any]]= mq.index(index_name).search(
        q=get_random_query(),
        reweight_score_param= reweight_score_param,
        random_weight_score = random_weight_score,
        searchable_attributes=["image"],
        attributes_to_retrieve=["image", "reputation", "word1", "word2"],
        limit=5
    )["hits"]

    return [
        {
            "image": r["image"],
            "reputation":  r["reputation"],
            "rank": i+1,
            "score": r["_score"],
            "uniq": f"{r['word1']}_{r['word2']}",
            "random": random_weight_score
        } for i, r in enumerate(results)
    ]

pprint(search(use_rep_score=True, random_weight_score=0))
# print()
# pprint(search(use_rep_score=True, random_weight_score=1))