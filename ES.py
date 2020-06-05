import requests
import json
import logging
logging.basicConfig(format='%(asctime)s %(message)s', level="INFO")


class ES:
    def __init__(self):
        self.base_uri = self.get_base_uri()
        self.uri = self.base_uri + '/mlspipeline_read/_search'
        self.uri_scroll = self.base_uri + '/_search/scroll'
        self.headers = {'Content-Type': 'application/json'}

    def get_base_uri(self):
        with open('config.json') as fid:
            data = json.load(fid)
        return data['uri']

    def scroll(self, scroll_query):
        logging.debug(f"scroll query {scroll_query}")
        query = json.dumps(scroll_query)
        response = requests.post(self.uri_scroll, data=query, headers=self.headers, timeout=10000)
        results = json.loads(response.text)
        return results

    def search(self, query, uri):
        # try:
        if uri is None:
            uri = self.uri
        logging.debug(f"search query {query}")
        query = json.dumps(query)
        response = requests.post(uri, data=query, headers=self.headers, timeout=10000)
        if response.status_code == requests.codes.ok:
            results = json.loads(response.text)
        else:
            logging.error(f"query {query} has status code {response.status_code} and return {response.text}")
            results = {}
        return results

    def update(self, query, uri):
        try:
            logging.debug(f"update query {query}")
            query = json.dumps(query)
            response = requests.post(uri, data=query, headers=self.headers, timeout=10000)
        except Exception as e:
            print("Exception occurred", e)

    def get_data_from_ES_prod_scroll(self, query):
        try:
            full_size = 0
            uri = self.uri + '?scroll=20m'
            alldata = []
            page = self.search(query, uri)
            alldata.append(page)
        except Exception as e:
            print("Exception occurred!", e)
            page = self.search(query, uri)
            alldata.append(page)
        sid = page['_scroll_id']
        done = 1
        scroll_size = page['hits']['total']
        print(f"scroll_size: {scroll_size}")
        if scroll_size < 10000:
            full_size = scroll_size
        else:
            full_size += 10000
        while scroll_size > 0:
            scroll_query = {"scroll": "20m", "scroll_id": sid}
            try:
                page = self.scroll(scroll_query)
                sid = page['_scroll_id']
                done += 1
            except Exception as e:
                print("Exception occurred", e)
                page = self.scroll(scroll_query)
                sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            alldata.append(page)
        return alldata
