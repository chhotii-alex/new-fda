import requests
import time

def classify_result(text):
    obj = {'text': text}
    for _ in range(3):
        try:
            r = requests.post('http://10.35.162.75:8000/classify', json=obj)
            return r.json()
        except:
            time.sleep(1)
    print(text)
    raise Exception("Could not get answer from server")

"""


from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from pathlib import Path

model_path = r"R:\Repo19\ArnaoutLab\Reagan_Udall_Foundation_grant\clinicalbert_finetuned_v3"

def init_model():
    # Load the tokenizer and model
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    # Set model to evaluation mode
    model.eval()
    return model, tokenizer

model, tokenizer = init_model()

def classify_result(text):
    # Tokenize input text
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    # Perform inference
    with torch.no_grad():
        outputs = model(**inputs)
    # Get predictions
    logits = outputs.logits
    predicted_class = torch.argmax(logits, dim=1).item()
    label_mapping = {0: 'positive', 1: 'equivocal', 2: 'unknown', 3: 'not done', 4: 'negative'}
    return label_mapping[predicted_class]

def test_model():
    # Example text for classification
    text = "Ran out of solution to perform the test. Delaying until the winter."
    sentiment = classify_result(text)
    print(f"Predicted sentiment: {sentiment}")


    """