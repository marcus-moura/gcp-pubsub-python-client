import json
from gcp_pub_sub import PubSub

# Specify the path to your JSON file
file_path = 'sales_data.json'

def main_publish(file_path: str):
    # Open and read the JSON file
    with open(file_path, 'r') as file:
        sales_data = json.load(file)

    #create publish
    publish = PubSub(sales_data)
    publish.producer()

if __name__== "__main__":
    main_publish(file_path)