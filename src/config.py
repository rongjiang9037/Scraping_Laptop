import configparser

def get_config(path = '/Users/margaret/OneDrive/Documents/projects/Scraping_Laptop/config.cfg'):
    config = configparser.ConfigParser()
    config.read(path)
    return config