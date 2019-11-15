import inspect

from IPython.display import display, Markdown as md

def code_to_md(code_text):
    display(md("""\n\n **Code Details:**\n\n---\n```python
{}
```\n---""".format(code_text)))
    
def wrapper_to_md(wrapper):
    try:
        wrapper_source = inspect.getsource(wrapper)
        display(md("""\n\n **Wrapper Details:**\n\n---\n```python
{}
```\n---""".format(wrapper_source)))
    except TypeError:
        raise Exception('Please do not use the str form of wrapper.')   
    