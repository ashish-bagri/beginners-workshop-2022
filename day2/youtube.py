import dlt
import requests


@dlt.source
def youtube_source(api_secret_key=dlt.secrets.value):
    return youtube_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers

def _paginated_get(url, headers, params, max_pages=5):
    """Requests and yields up to `max_pages` pages of results as per Twitter API docs: https://developer.twitter.com/en/docs/twitter-api/pagination"""
    while True:        
        response = requests.get(url, params=params)        
        response.raise_for_status()
        page = response.json()
        # show the pagination info
        meta = page["nextPageToken"]
        print(meta)

        yield page

        # get next page token
        next_token = meta
        max_pages -= 1

        # if no more pages or we are at the maximum
        if not next_token or max_pages == 0:
            break
        else:
            # set the next_token parameter to get next page
            params['pageToken'] = next_token

@dlt.resource(write_disposition="append")
def youtube_resource(api_secret_key=dlt.secrets.value):
    #headers = _create_auth_headers(api_secret_key)

    # check if authentication headers look fine
    #print(headers)

    # make an api call here
    
    url ="https://youtube.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "maxResults": 25,
        "q": "python",     
        "key": api_secret_key
    }
    #response = requests.get(url, params=params)
    #response.raise_for_status()
    response = _paginated_get(url=url, headers=None, params=params)
    for page in response:
        yield page
    #yield response.json()


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='youtube', destination='bigquery', dataset_name='youtube_data')

    # print credentials by running the resource
    data = list(youtube_resource())

    # print the data yielded from resource
    print(data)
    #exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(youtube_source())

    # pretty print the information on data that was loaded
    print(load_info)
