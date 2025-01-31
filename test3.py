import os
import time
import google.generativeai as genai
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")



def load_json_file(path):
  """Loads a JSON file from the given path."""
  try:
    with open(path, 'r', encoding='utf-8') as f:
      return json.load(f)
  except FileNotFoundError:
    print(f"Error: File not found at path '{path}'")
    return None
  except json.JSONDecodeError:
    print(f"Error: Invalid JSON format in file '{path}'")
    return None

def transform_json_structure(file_to_transform, file_reference, model):
    """Transforms the structure of file_to_transform based on file_reference using Gemini."""
    if file_to_transform is None or file_reference is None:
        return "Error: One or both files are missing or invalid."

    # Create the chat session for the transformation task
    chat_session = model.start_chat(
    history=[
        {
            "role": "user",
            "parts": [
                json.dumps(file_to_transform),
                json.dumps(file_reference),
                "Soit ces deux fichiers json. Tu transforme la structure du premier fichier en te basant de la structure du deuxième fichier\n",
            ],
        },
       
    ]
  )

    # Get the response from the model
    response = chat_session.send_message("Transform json")
    try:
      # Try to load the response text as JSON
        transformed_json = json.loads(response.text)
        return transformed_json
    except json.JSONDecodeError:
      # If not valid JSON, return the response text as is
       return response.text

# Configuration of the model
generation_config = {
  "temperature": 1,
  "top_p": 0.95,
  "top_k": 40,
  "max_output_tokens": 8192,
  "response_mime_type": "text/plain",
}

model = genai.GenerativeModel(
  model_name="gemini-2.0-flash-exp",
  generation_config=generation_config,
)
  

# Example usage:
if __name__ == "__main__":
  file_to_transform_path = "cvs_fin.json"  
  file_reference_path = "cv10.json"  

  # Load JSON files
  file_to_transform = load_json_file(file_to_transform_path)
  file_reference = load_json_file(file_reference_path)

  # Transform the structure using Gemini
  transformed_data = transform_json_structure(file_to_transform, file_reference, model)

  if isinstance(transformed_data,dict) or isinstance(transformed_data, list) :
    print("Transformed JSON:\n", json.dumps(transformed_data, indent=4, ensure_ascii=False))
  else:
    print("Transformed data:\n",transformed_data)