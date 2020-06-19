import pandas as pd
from .api import API, DataView

class ORESAPI(API):

    """Summary

    Attributes:
        project (TYPE): Description
    """

    def __init__(self, lng='en',
                 domain: str='ores.wikimedia.org',
                 project: str='wiki',
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
        
        self.base = f"{self.base}/v3/scores/{lng + project}"
        
    def get_goodfaith_damage(self, rev_list):
        rev_ids = '|'.join(rev_list)
        
        return self.request(f'{self.base}?models=goodfaith%7Cdamaging&revids={rev_ids}')
    
    
class ORESDV(DataView):
    
    def get_goodfaith_damage(self, rev_list):
        res = self.api.get_goodfaith_damage(rev_list)
        
        ores_df = pd.DataFrame(columns=["rev_id", "Damaging", "Goodfaith"])
        for idx, rev in enumerate(rev_list):   
            one_rev_dict = res["enwiki"]["scores"][rev]
            df_dict = {}
            for k, v in one_rev_dict.items():
                df_dict[k] = v["score"]["probability"]["true"]
            row = [rev] + list(df_dict.values())
            ores_df.loc[idx] = row
            
        return ores_df
        