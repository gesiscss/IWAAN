
import datetime
import pandas as pd
from .api import API, DataView


class WikiMediaDV(DataView):

    def get_pageviews(self, article_name: str, granularity: str = 'monthly'):
        """Get pageview counts for an article

        Args:
            article_name (str): The title of any article in the specified project. Any spaces should be
                replaced with underscores. It also should be URI-encoded, so that non-URI-safe characters 
                like %, / or ? are accepted. Example: Are_You_the_One%3F
            granularity (str, optional): The time unit for the response data. As of today, the only supported granularity 
            for this endpoint is `daily` and `monthly`.

        Returns:
            dict: ageview counts for an article
        """

        df = pd.DataFrame(data=self.api.get_pageviews(article_name, granularity)['items'])
        df['timestamp'] = pd.to_datetime(df['timestamp'].str[:8])

        return df


class WikiMediaAPI(API):

    """Summary

    Attributes:
        project (TYPE): Description
    """

    def __init__(self, lng: str='en',
                 domain: str='wikimedia.org',
                 project: str='wikipedia.org',
                 version: str='rest_v1',
                 api_username: str=None,
                 api_password: str=None,
                 api_key: str=None,
                 protocol: str='https',
                 attempts: int=2):
        """Constructor of the WikiWhoAPI

        Args:
            domain (str, optional): the domain that hosts the api
            project (str, optional): e.g. en.wikipedia.org
            version (str, optional): version of the API (e.g. rest_v1)
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
        self.base = f'{self.base}api/{version}/'
        self.project = lng + '.' + project

    def get_pageviews(self, article_name: str, granularity: str = 'monthly'):
        """Get pageview counts for an article

        Args:
            article_name (str): The title of any article in the specified project. Any spaces should be
                replaced with underscores. It also should be URI-encoded, so that non-URI-safe characters 
                like %, / or ? are accepted. Example: Are_You_the_One%3F
            granularity (str, optional): The time unit for the response data. As of today, the only supported granularity 
            for this endpoint is `daily` and `monthly`.

        Returns:
            dict: ageview counts for an article
        """
        start = 19900101
        today = datetime.date.today().strftime("%Y%m%d")
        end = int(today)

        url = (f'{self.base}metrics/pageviews/per-article/{self.project}/'
                f'all-access/all-agents/{article_name}/{granularity}/{start}/{end}')

        return self.request(url)
