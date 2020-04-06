import requests


import os
import requests

import pandas as pd


class API:

    """The APIs provide provenance and change information about the tokens a Wikipedia article consists of, for several languages. Apart from the source language edition they draw from, their specifications and usage are identical
    Attributes:
        attempts (int): Number of attempts to be done to the server
        base (url): Base request url
    """

    def __init__(self, lng: str,
                 domain: str,
                 api_username: str=None,
                 api_password: str=None,
                 api_key: str=None,
                 api_key_name: str=None,
                 protocol: str="https",
                 attempts: int=2):
        """Constructor of the WikiWhoAPI

        Args: 
            lng (str): the language used for api
            domain (str): the domain that hosts the api
            api_username (str, optional): API username
            api_password (str, optional): API password
            api_key (str, optional): API key
            api_key_name (str, optional): The name of the dictionary key of the API key in the 
                parameters, e.g. `session.params[api_key_name] = api_key` 
            protocol (str, optional): the protocol of the url
            attempts (int, optional): the number of attempts before giving up trying to connect

        """

        self.session = requests.Session()
        if api_username and api_password:
            self.session.auth = (api_username, api_password)

        if api_key:
            self.session.params = {}
            self.session.params[api_key_name] = api_key

        self.attempts = attempts
        if domain == 'wikipedia.org':
            self.base = f'{protocol}://{lng}' + '.' + f'{domain}/'
        else:
            self.base = f'{protocol}://{domain}/'

    def request(self, url: str) -> dict:
        """Do the request

        Args:
            url (str): The request url

        Returns:
            dict: The results of the request

        Raises:
            exc: If a connection has failed
        """

        for attempt in range(0, self.attempts + 1):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                if attempt == self.attempts:
                    raise exc
                else:
                    print(f"Request ({url}) failed (attempt {attempt + 1} of {self.attempts}) ")



class DataView:

    """Query methods for correspondence of the API methods 
    """
    
    def __init__(self, api: API):
        """Constructor of the DataView
        
        Args:
            api (API): the WikiWhoAPI
        """
        self.api = api
