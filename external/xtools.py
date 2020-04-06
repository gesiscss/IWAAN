import pandas as pd

from .api import API, DataView


class XtoolsDV(DataView):

    def get_page_info(self, page: str) -> pd.Series:
        """Get pageview counts for an page

        Args:
            page_id (str): Description

        Returns:
            pd.Series: info of the page

        """

        res = self.api.get_page_info(page)

        return pd.Series(res)

    def get_modified_pages_counts_per_editor(self, editor):

        res = self.api.get_modified_pages_counts_per_editor(editor)

        # return pd.Series(res)

        count = res['counts']
        if len(count) == 0:
            raise Exception('Not Found')

        # elif len(count) > 1:
        #     raise Exception('Several Found')

        return pd.Series({
            'Created pages:': count['count'],
            'Deleted pages': count['deleted'],
            #'Redirected pages': count['redirects']
        })

    def get_created_pages_per_editor(self, editor):

        res = self.api.get_created_pages_per_editor(editor)

        pages = res['pages']
        if len(pages) == 0:
            raise Exception('Not Found')

        return pd.DataFrame(res['pages'])


class XtoolsAPI(API):

    """Summary

    Attributes:
        project (TYPE): Description
    """

    def __init__(self, lng='en',
                 domain: str='xtools.wmflabs.org',
                 project: str='wikipedia.org',
                 api_username: str=None,
                 api_password: str=None,
                 api_key: str=None,
                 protocol: str='https',
                 attempts: int=2):
        """Constructor of the WikiWhoAPI

        Args:
            lng (str): the language used for api, e.g. en
            domain (str, optional): the domain that hosts the api
            project (str, optional): e.g. wikipedia.org
            api_username (str, optional): WikiWho API username
            api_password (str, optional): WikiWho API password
            api_key (str, optional): WikiWho API key
            protocol (str, optional): the protocol of the url
            attempts (int, optional): the number of attempts before giving up trying to connect
        """
        super().__init__(protocol=protocol,
                         lng=lng,
                         domain=domain,
                         api_username=api_username,
                         api_password=api_password,
                         api_key=api_key,
                         attempts=attempts)
        self.project = lng + '.' + project
        self.base = f"{self.base}api/"

    def get_page_info(self, page_name: str) -> dict:
        """Get basic information about the history of a page.

        Args:
            page_name (str): Full page title.

        Returns:
            dict: basic information about the history of a page.
        """

        return self.request(f'{self.base}page/articleinfo/{self.project}/{page_name}')

    def get_modified_pages_counts_per_editor(self, editor_name: str) -> dict:
        """Get basic information about the history of a page.

        Args:
            editor_name (str): Full page title.

        Returns:
            dict: basic information about the history of a page.
        """
        print(f'{self.base}user/pages_count/{self.project}/{editor_name}')

        return self.request(f'{self.base}user/pages_count/{self.project}/{editor_name}')

    def get_created_pages_per_editor(self, editor_name: str) -> dict:
        """Get basic information about the history of a page.

        Args:
            editor_name (str): Full page title.

        Returns:
            dict: basic information about the history of a page.
        """

        return self.request(f'{self.base}user/pages/{self.project}/{editor_name}')
