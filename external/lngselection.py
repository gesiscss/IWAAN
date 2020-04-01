def abbreviation(lng):
    """lng (str): Language name."""
    if lng == 'Türkçe':
        return 'tr'
    else:
        return lng[:2].lower()