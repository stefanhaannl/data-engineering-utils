import requests

from .base import Connection

import polars as pl


class RestApi(Connection):

    connection_type = "RestApi"

    def __init__(self, base_path: str):
        super().__init__()
        self.base_path = base_path.rstrip('/')
        self.headers = {"Accept": "application/json"}


    def get(self, endpoint: str, parameters: dict | None = None) -> dict | None:
        url = f"{self.base_path}/{endpoint.lstrip('/')}"
        response = requests.get(url, params=parameters, headers=self.headers)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return response.json()


    def post(self, endpoint: str, data: dict | None = None, json: dict | None = None) -> dict | None:
        url = f"{self.base_path}/{endpoint.lstrip('/')}"
        response = requests.post(url, data=data, json=json, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_page(self, endpoint: str, parameters: dict | None = None, page: int = 1, size: int = 100) -> dict | None:
        if not parameters:
            parameters = {}
        parameters["page"] = page
        parameters["pageSize"] = size
        return self.get(endpoint, parameters)

    def get_all_pages(self, endpoint: str, parameters: dict | None = None, size: int = 100) -> list:

        json_list = []
        page = 1

        while True:
            json = self.get_page(endpoint, parameters, page=page, size=size)
            if not json:
                break
            json_list.append(json)
            page += 1

        return json_list
