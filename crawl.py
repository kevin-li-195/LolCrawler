from lolcrawler.lolcrawler import LolCrawler, TopLolCrawler
from riotwatcher import RiotWatcher
from config import config
from pymongo import MongoClient
from datetime import date, timedelta
from clint import arguments

ALL_REGIONS = [
        "na1",
        "ru",
        "kr",
        "br1",
        "oc1",
        "jp1",
        "eun1",
        "euw1",
        "tr1",
        "la1",
        "la2"
        ]

if __name__=="__main__":

    args = arguments.Args()
    action = args.get(0)

    ## Connect to MongoDB database
    client = MongoClient(config['mongodb']['host'], config['mongodb']['port'])
    db = client[config['mongodb']['db']]

    ## Initialise Riot API wrapper
    api_key = config['api_key']
    if api_key=='':
        raise LookupError("Provide your API key in config.py")

    region=config["region"]

    # if config['is_production_key']:
    #     limits = (Limit(3000,10), Limit(180000,600), )
    # else:
    #     limits = (Limit(10, 10), Limit(500, 600), )

    api = RiotWatcher(config['api_key'])

    if action=="top":
        crawler = TopLolCrawler(api,db_client=db, include_timeline=config["include_timeline"], region= "na1")
        crawler.start(regions=['euw1', 'na1', 'kr', 'eun1'], leagues=['grandmaster'])
    else:
        ## Initialise crawler
        crawler =  LolCrawler(api, db_client=db, include_timeline=config["include_timeline"], region = "na1")
        crawler.start(config['summoner_seed_name'])
