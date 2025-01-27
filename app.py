import pandas as pd
import requests
import logging
import argparse
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SWAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_json(self, endpoint: str) -> list:
        all_data = []
        url = f"{self.base_url}{endpoint}/"

        while url:
            #vv
            logger.info(f"Отримання даних з: {url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            all_data.extend(data['results'])
            url = data.get('next')

        return all_data

class SWAPIDataManager:
    def __init__(self, client: SWAPIClient):
        self.client = client
        self.data = {}

    def fetch_entity(self, endpoint: str):
        raw_data = self.client.fetch_json(endpoint)
        self.data[endpoint] = pd.DataFrame(raw_data)

    def apply_filter(self, endpoint: str, columns_to_drop: list):
        if endpoint in self.data:
            self.data[endpoint].drop(columns=columns_to_drop, inplace=True)

    def save_to_excel(self, filename: str):
        with pd.ExcelWriter(filename) as writer:
            for endpoint, df in self.data.items():
                df.to_excel(writer, sheet_name=endpoint.capitalize(), index=False)
        logger.info(f"Дані успішно записано у файл: {filename}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="SWAPI Data Manager")
    parser.add_argument('--endpoint', type=str, required=True, help="Список ендпоінтів через кому, наприклад, people,planets")
    parser.add_argument('--output', type=str, required=True, help="Назва вихідного Excel-файлу")
    parser.add_argument('--filters-file', type=str, help="Шлях до файлу filter.json з фільтрами")
    return parser.parse_args()

def load_filters(filters_file):
    if filters_file:
        try:
            with open(filters_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Помилка при зчитуванні файлу фільтрів: {e}")
            return {}
    return {}

def main():
    args = parse_arguments()

    client = SWAPIClient(base_url="https://swapi.dev/api/")
    manager = SWAPIDataManager(client)

    endpoints = args.endpoint.split(',')
    filters = load_filters(args.filters_file)

    for endpoint in endpoints:
        logger.info(f"Завантаження даних для ендпоінту: {endpoint}")
        manager.fetch_entity(endpoint)

        if endpoint in filters:
            manager.apply_filter(endpoint, filters[endpoint])

    manager.save_to_excel(args.output)

if __name__ == "__main__":
    main()
