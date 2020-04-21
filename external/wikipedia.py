"""Summary
"""
from typing import Union

import pandas as pd
import numpy as np

from .api import API, DataView
from .utils import chunks
from itertools import chain
from urllib.parse import quote_plus


class WikipediaDV(DataView):
    """Summary
    """

    def get_page(self, page: Union[int, str]) -> pd.Series:
        """Get pageview counts for an page

        Args:
            page (Union[int, str]): Description

        Returns:
            pd.Series: info of the page

        Deleted Parameters:
            page_id (Union[int, str]): Description

        Raises:
            Exception: Description

        """

        res = self.api.get_page(page)

        pages = res['query']['pages']
        if len(pages) == 0:
            raise Exception('Article Not Found')

        elif len(pages) > 1:
            raise Exception('Several Pages Found')

        page_dict = next(iter(pages.values()))

        return pd.Series({
            'page_id': page_dict['pageid'],
            'title': page_dict['title'],
            'ns': page_dict['ns'],
        })

    def get_editor(self, editor: Union[int, str]) -> pd.Series:
        """Summary

        Args:
            editor (Union[int, str]): Description

        Returns:
            pd.Series: info of editor

        Raises:
            Exception: Description
        """
        res = self.api.get_editor(editor)

        editors = res['query']['users']
        if len(editors) == 0:
            raise Exception('Editor Not Found')

        elif len(editors) > 1:
            raise Exception('Several Editors Found')

        return pd.Series(editors[0])

    def search_page(self, search_query: str) -> pd.Series:
        """Summary

        Args:
            search_query (str): Description

        Returns:
            pd.Series: page title

        Raises:
            Exception: Description
        """
        res = self.api.search_page(search_query)

        result = res[1]
        if len(result) == 0:
            raise Exception('Article Not Found')

        elif len(result) > 1:
            raise Exception('Several Pages Found')

        # return pd.Series({
        #     'title': result[0]
        # })

        return result[0]

    def get_editors(self, editors: list) -> pd.Series:

        res = (self.api.get_editors(chunk)['query'][
               'users'] for chunk in chunks(editors, 50))

        return pd.DataFrame(x for x in chain(*res))
    
    def get_talk_content(self, pageid: Union[int, str]) -> pd.Series:

        res = self.api.get_talk_content(pageid) 
        talk_content = next(iter(res["query"]["pages"].values()))

        return pd.DataFrame(talk_content["revisions"])
    
    def get_talk_rev_diff(self, fromrev, torev) -> pd.Series:

        res = self.api.get_talk_rev_diff(fromrev, torev) 
        talk_diff = pd.Series(next(iter(res.values()))).rename(columns={"*":"content"})

        return talk_diff


class WikipediaAPI(API):
    """Summary

    Attributes:
        base (TYPE): Description

    Deleted Attributes:
        project (TYPE): Description
    """

    def __init__(self, lng: str = 'en',
                 domain: str = 'wikipedia.org',
                 api_username: str = None,
                 api_password: str = None,
                 api_key: str = None,
                 protocol: str = 'https',
                 attempts: int = 2):
        """Constructor of the WikiWhoAPI

        Args:
            domain (str, optional): the domain that hosts the api
            api_username (str, optional): WikiWho API username
            api_password (str, optional): WikiWho API password
            api_key (str, optional): WikiWho API key
            protocol (str, optional): the protocol of the url
            attempts (int, optional): the number of attempts before giving up trying to connect

        Deleted Parameters:
            project (str, optional): e.g. en.wikipedia.org
            version (str, optional): version of the API (e.g. rest_v1)
        """
        super().__init__(protocol=protocol,
                         lng=lng,
                         domain=domain,
                         api_username=api_username,
                         api_password=api_password,
                         api_key=api_key,
                         attempts=attempts)
        self.base = f'{self.base}w/api.php?'

    def get_page(self, page: Union[int, str]) -> dict:
        """Get pageview counts for an page

        Args:
            page (Union[int, str]): Description

        Returns:
            dict: ageview counts for an page

        """

        if isinstance(page, (int, np.integer)):
            url = f'{self.base}action=query&pageids={page}&format=json'
        elif isinstance(page, str):
            url = f'{self.base}action=query&titles={quote_plus(page)}&format=json'

        return self.request(url)

    def get_editor(self, editor: Union[int, str]) -> dict:
        """Get pageview counts for an page

        Args:
            editor (Union[int, str]): Description

        Returns:
            dict: ageview counts for an page

        """

        # if isinstance(editor, (int, np.integer)):
        #     url = f'{self.base}action=query&list=users&ususerids={editor}&format=json'
        # elif isinstance(editor, str):
        # url =
        # f'{self.base}action=query&list=users&ususers={editor}&format=json'

        if isinstance(editor, (int, np.integer)):
            url = f'{self.base}action=query&list=users&ususerids={editor}&usprop=blockinfo|editcount|registration|gender&format=json'
        elif isinstance(editor, str):
            url = f'{self.base}action=query&list=users&ususers={quote_plus(editor)}&usprop=blockinfo|editcount|registration|gender&format=json'

        return self.request(url)

    def search_page(self, search_query: str) -> dict:
        """Summary

        Args:
            search_query (str): Description

        Returns:
            dict: Description
        """
        url = f'{self.base}action=opensearch&search={quote_plus(search_query)}&limit=1&namespace=0&format=json'

        return self.request(url)

    def get_editors(self, editors: list) -> dict:

        editors_str = "|".join(quote_plus(str(x)) for x in editors)

        if isinstance(editors[0], (int, np.integer)):
            url = f'{self.base}action=query&list=users&ususerids={editors_str}&usprop=blockinfo|editcount|registration|gender&format=json'
        elif isinstance(editors[0], str):
            url = f'{self.base}action=query&list=users&ususers={editors_str}&usprop=blockinfo|editcount|registration|gender&format=json'

        return self.request(url)
    
    def get_talk_content(self, pageid: Union[int, str]) -> dict:
        url = f'{self.base}action=query&format=json&prop=revisions&rvlimit=max&rvprop=timestamp|ids|user|comment&pageids={pageid}'

        return self.request(url)
    
    def get_talk_rev_diff(self, fromrev, torev) -> dict:
        url = f'{self.base}action=compare&format=json&fromrev={fromrev}&torev={torev}'

        return self.request(url)
