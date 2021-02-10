import requests
import os
import math
import json
import base64
from datetime import date, timedelta
from google.cloud import storage

storage_client = storage.Client()

# Google Cloud Storage Bucket was set as a run-time environment variables were set in the creation of the Cloud Function on GCP.
STORAGE_BUCKET = os.environ['STORAGE_BUCKET']

# Create a function that takes 'url' as the parameter
def get_gallery_data(url):
    
    # Fixing the max number of results returned from the API to 500 to keep request low as the gallery grows with time.
    # The default value for count is 12.
    count = 500
    
    # Setting an empty list for 'vizzes' which is where you will end up dumping API output.
    vizzes = []
    
    # Setting an empty list for 'totalItems' which is where you will end up storing the integer for number of vizzes in the gallery.
    totalItems = []
    
    # Response from the url, which is an API, will be in JSON format.
    response = requests.get(url).json()
    
    # Get the total number 
    totalItems = response['totalItems']

    # Since the API is paginated, we can calculate the number of requests we need to make by dividing the number of totalItems by the count of results per request.
    pagesNeeded = math.ceil(totalItems / count)
    
    # Make multiple requests by looping through the number of pagesNeeded
    i = 0
    while i <= pagesNeeded:
        response = requests.get(str(url) + "?&count=" + str(count) + "&page=" + str(i)).json()
        vizzes.extend(response['items'])
        i = i + 1
    return(vizzes)

# Create a function that will execute the function above and store in a specified GCS Bucket.
def fetch_and_write(data, context):
    action = base64.b64decode(data['data']).decode('utf-8')
    yesterday = date.today() - timedelta(1)
    yesterday = str(yesterday)[:10]
    url = 'https://public.tableau.com/api/gallery'
    
    # This is a Pub/Sub message on Cloud Scheduler that sends a message on a specified schedule, acting as the trigger for executing this function.
    if (action == "download!"):
        payload = get_gallery_data(url)
        payload = '\n'.join(json.dumps(item) for item in payload)

        # Saving the file with yesterday's date in the name for archival
        file_name = "PublicGalleryData_{}.json".format(yesterday.replace("-", ""))
        storage_client.get_bucket(STORAGE_BUCKET) \
            .blob(file_name) \
            .upload_from_string(payload)
    else:
        print("No instructions received.")    
