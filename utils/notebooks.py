from ipywidgets.widgets import SelectionRangeSlider
from notebook import notebookapp
import pandas as pd
import urllib
import json
import os
import ipykernel
import glob

def notebook_path():
    """Returns the absolute path of the Notebook or None if it cannot be determined
    NOTE: works only when the security is token-based or there is also no password
    """
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  # No token and no password, ahem...
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+'api/sessions?token='+srv['token'])
            sessions = json.load(req)
            for sess in sessions:
                if sess['kernel']['id'] == kernel_id:
                    return os.path.join(srv['notebook_dir'],sess['notebook']['path'])
        except:
            pass  # There may be stale entries in the runtime directory 
    return None

def notebook_name():
    """Returns the name of the Notebook or None if it cannot be determined
    NOTE: works only when the security is token-based or there is also no password
    """
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[1].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  # No token and no password, ahem...
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+'api/sessions?token='+srv['token'])
            sessions = json.load(req)
            for sess in sessions:
                if sess['kernel']['id'] == kernel_id:
                    return sess['notebook']['path']
        except:
            pass  # There may be stale entries in the runtime directory 
    return None

def get_next_notebook():
    try:
        _id = int(notebook_name()[0]) + 1
    except:
        _id = int(notebook_name()[1]) + 1
    return glob.glob(f"{_id}*.ipynb")[0]


def get_previous_notebook():
    try:
        _id = int(notebook_name()[0]) - 1
    except:
        _id = int(notebook_name()[1]) - 1
    return glob.glob(f"{_id}*.ipynb")[0]

def get_notebook_by_number(_id):
    return glob.glob(f"{_id}*.ipynb")[0]


def get_date_slider_from_datetime(datetime_series):  
    datetime_series= datetime_series[~datetime_series.isnull()]
    opts = datetime_series.sort_values().dt.date.unique()

    if len(opts) == 1:
        opts = [pd.Timestamp(0).date(), opts[0]]
    elif len(opts) == 0:
        opts = [pd.Timestamp(0).date(), pd.Timestamp.today().date()]
        
    return SelectionRangeSlider(
        options = opts,
        index = (0, len(opts)-1),
        description='Date Range',
        continuous_update=False,
        layout={'width': '500px'}
    )



