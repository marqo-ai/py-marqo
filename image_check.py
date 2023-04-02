from typing import List, Dict, Any, Optional
from PIL import Image
import marqo
import random
import requests
import io

words = requests.get("https://www.mit.edu/~ecprice/wordlist.10000").text.splitlines()
def get_random_query() -> str:
    """Get a random 5 word query from the vocabulary.
    """
    return " ".join(random.sample(words, k=5))

def search(n: int, rep_w: int = 0, random_weight_score: int = 0, q: Optional[str] = None) -> List[Dict[str, Any]]:
    index_name="image-scoring-index"
    mq = marqo.Client(url='http://localhost:8882')
    try:
        q = q if q is not None else get_random_query()
        results: List[Dict[str, Any]]= mq.index(index_name).search(
            q=q,
            reweight_score_param= "reputation",
            reputation_weight_score=rep_w,
            random_weight_score = random_weight_score,
            searchable_attributes=["image"],
            attributes_to_retrieve=["image", "reputation", "word1", "word2"],
            limit=n
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

def create_reputation_image_lists(q: str, reputations: List[float], results: int = 5) -> List[List[Image.Image]]:
    """Create a list of image lists for each reputation value.
    """
    image_lists = []
    for rep_w in reputations:
        image_lists.append(create_images(rep_w, q, results))

    return image_lists

def create_images(reputation: float, query: str, count: int) -> List[Image.Image]:
    results = search(count, rep_w=reputation, q=query)
    
    images = []
    for result in results:
        image_url = result["image"]
        response = requests.get(image_url)
        img = Image.open(io.BytesIO(response.content))
        images.append(img)
    return images

def create_aggregate_image(image_lists: List[List[Image.Image]]) -> Image.Image:
    print(image_lists)
    print([len(x) for x in image_lists])
    # Combine the image lists into a single image
    max_width = sum(img.size[0] for img in image_lists[0])
    total_height = sum(max(img.size[1] for img in images) for images in image_lists)
    
    print([max(img.size[0] for img in images) for images in image_lists])
    print([max(img.size[1] for img in images) for images in image_lists])
    print(max_width, total_height)

    combined_image = Image.new('RGB', (max_width, total_height))

    y_offset = 0
    for images in image_lists:
        x_offset = 0
        max_height = max(img.size[1] for img in images)
        for img in images:
            print(x_offset, y_offset)
            combined_image.paste(img, (x_offset, y_offset))
            x_offset += img.size[0]
        y_offset += max_height

    # Display the combined image
    return combined_image

def save_a_b_test(q: str, reps: List[float]):
    create_aggregate_image(
        create_reputation_image_lists(
            q=q,
            reputations=reps,
            results=10
        )
    ).save(f"{q.replace(' ', '_')}_{'_'.join([str(x) for x in reps])}.png")

save_a_b_test("green person with a hat", [0.0, 0.2, 0.4])
save_a_b_test("purple person smoking with a dog", [0.0, 0.2, 0.4])
save_a_b_test("evil clown with a spear", [0.0, 0.2, 0.4])
save_a_b_test("angry romans", [0.0, 0.2, 0.4])
save_a_b_test("cybord artist", [0.0, 0.2, 0.4])