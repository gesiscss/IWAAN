import json

def abbreviation(lng):
    """lng (str): Language name."""
    if lng == 'Türkçe':
        return 'tr'
    else:
        return lng[:2].lower()
    
def lng_listener(lng, search_term):
    new_dict = {'lng': lng, 'search_term': search_term}
    with open('utils/language.json', 'w', encoding='utf-8') as file:
        json.dump(new_dict, file)