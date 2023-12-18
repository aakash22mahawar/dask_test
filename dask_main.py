import logging
import httpx
from bs4 import BeautifulSoup
from dask.distributed import Client
import dask
import time
from snowflake_connector import SnowflakeConnector

class QuestionScraper:
    def __init__(self, urls):
        self.urls = urls
        self.logger = self.setup_logger()
        self.scraped_items = []

    def setup_logger(self):
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                                datefmt='%d-%m-%Y %H:%M:%S')
            return logging.getLogger(__name__)

    def get_question(self, url):
        with httpx.Client(timeout=10) as client:
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}
            resp = client.get(url, headers=header)
            time.sleep(2)  # Introduce a delay after each request

            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                return [x.text.strip() for x in soup.select('h3.s-post-summary--content-title')]
            else:
                self.logger.warning(f"Failed to retrieve data from {url}. Status code: {resp.status_code}")

    def scrape_questions(self):
        with Client(n_workers=8, threads_per_worker=5) as client:
            urls_distributed = client.scatter(self.urls)
            results = dask.compute(*[dask.delayed(self.get_question)(url) for url in urls_distributed])

        for result in results:
            for q in result:
                item = {'question': q}
                self.scraped_items.append(item)
                self.logger.info(f'++++ item has been scraped ++++')

    def insert_into_snowflake(self, batch_size=50):
        snowflake_connector = SnowflakeConnector()

        for i in range(0, len(self.scraped_items), batch_size):
            batch = self.scraped_items[i:i+batch_size]
            snowflake_connector.insert_data(batch)
            print(f'++++++++++++++++ item_batch has been pushed into Snowflake +++++++++++++++++')

        snowflake_connector.close_connection()

    def main(self):
        self.scrape_questions()
        self.insert_into_snowflake()

if __name__ == "__main__":
    start = time.time()
    scraper = QuestionScraper([f"https://stackoverflow.com/questions/tagged/python?tab=newest&pagesize=50&page={page_num}" for page_num in range(1, 101)])
    scraper.main()
    end = time.time()
    print(f'**** time elapsed {round(end - start, 2)} ****')
