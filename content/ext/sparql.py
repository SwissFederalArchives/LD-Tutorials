import json
import pandas as pd
from pyodide.ffi import to_js
from IPython.display import JSON, HTML
from js import Object, fetch
from io import StringIO

async def query(query_string, store = "L", set_na = False):
    
    # three Swiss triplestores
    if store == "F":
        # as there is a CORS problem at the moment with FEDLEX endpoint, the LINDAS endpoint with a SERVICE clause is used 
        # address = 'https://fedlex.data.admin.ch/sparqlendpoint'
        address = 'https://ld.admin.ch/query'
    elif store == "G":
        address = 'https://geo.ld.admin.ch/query'
    elif store == "L":
        address = 'https://ld.admin.ch/query'
    else:
        address = store

    # build the query
    if store == "F":
        query_string = insert_service_tail(insert_service_head(query_string))
    
    # try the Post request with help of JS fetch
    # the creation of the request header is a little bit complicated because it needs to be a 
    # JavaScript JSON that is made within a Python source code
    try:
        resp = await fetch(address,
          method="POST",
          body="query=" + query_string.replace("+", "%2B").replace("&", "%26"),
          #credentials="same-origin",
          headers=Object.fromEntries(to_js({"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", 
                                            "Accept": "text/csv" })),
        )
    except:
        raise RuntimeError("fetch failed")
    
    
    if resp.ok:
        res = await resp.text()
        # ld.admin.ch throws errors starting with '{"message":'
        if '{"message":' in res:
            error = json.loads(res)
            raise RuntimeError("SPARQL query malformed: " + error["message"])
        # geo.ld.admin.ch throws errors starting with 'Parse error:'
        elif 'Parse error:' in res:
            raise RuntimeError("SPARQL query malformed: " + res)
        else:
            # if everything works out, create a pandas dataframe from the csv result
            df = pd.read_csv(StringIO(res), na_filter = set_na)
            return df
    else:
        # fedlex.data.admin.ch throws error with response status 400
        if resp.status == 400:
            raise RuntimeError("Response status 400: Possible malformed SPARQL query. No syntactic advice available.")
        else:
            raise RuntimeError("Response status " + str(resp.status))
            
            
def display_result(df):
    df = HTML(df.to_html(render_links=True, escape=False))
    display(df)

# helper functions to insert SERVICE clause
def insert_service_head(original_string):
    
    # Find the index of the first occurrence of '{'
    index = original_string.find('{')
    
    # Check if '{' is found
    if index == -1:
        # If '{' is not found, return the original string unchanged
        return original_string
    
    # Split the string into two parts
    before_bracket = original_string[:index + 1]
    after_bracket = original_string[index + 1:]
    
    # Concatenate the parts with the substring inserted in between
    result_string = before_bracket + 'SERVICE <https://fedlex.data.admin.ch/sparqlendpoint> {' + after_bracket
    
    return result_string

def insert_service_tail(original_string):
    
    # Find the index of the last occurrence of '}'
    index = original_string.rfind('}')
    
    # Check if '}' is found
    if index == -1:
        # If '}' is not found, return the original string unchanged
        return original_string
    
    # Split the string into two parts
    before_bracket = original_string[:index + 1]
    after_bracket = original_string[index + 1:]
    
    # Concatenate the parts with the additional '}' inserted in between
    result_string = before_bracket + '}' + after_bracket
    
    return result_string
