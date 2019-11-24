import sys
import time
import numpy as np
from datetime import datetime
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError
import pymongo
import logging
import time
from datetime import datetime, date, timedelta
import itertools
from requests.exceptions import SSLError, HTTPError
from unidecode import unidecode

from .extract_match import extract_match_infos

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

# create a file handler
handler = logging.FileHandler('crawler.log')
handler.setLevel(logging.DEBUG)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)


MATCHLIST_COLLECTION = "matchlist"
MATCH_COLLECTION = "match"
MATCHLIST_PAGE_LIMIT = 60


class LolCrawlerBase():
    """Crawler base class for all crawlers"""

    def __init__(self, api, db_client, region, include_timeline=False):
        self.api = api
        self.include_timeline = include_timeline
        ## Stack of summoner ids to crawl
        self.summoner_names = []
        self.summoner_names_done = []
        ## Stack of match ids to crawl
        self.match_ids = []
        self.match_ids_done = []
        self.db_client = db_client
        self.region = region

    def _store(self, identifier, entity_type, entity, upsert=False):
        """Stores matches and matchlists"""
        entity.update({'_id': identifier})
        try:
            if upsert:
                self.db_client[entity_type].replace_one(filter={"_id": identifier}, replacement = entity, upsert=True)
            else:
                self.db_client[entity_type].insert_one(entity)
        except DuplicateKeyError:
            logger.warning("Duplicate: Mongodb already inserted {entity_type} with id {identifier}".format(entity_type=entity_type, identifier=identifier))
        except ServerSelectionTimeoutError as e:
            logger.error("Could not connect to Mongodb", exc_info=True)
            sys.exit(1)

    def flush(self):
        self.summoner_names = []
        self.match_ids = []

    def crawl_matchlist(self, summoner_name):
        """Crawls matchlist of given summoner,
        stores it and saves the matchIds"""
        logger.debug('Getting partial matchlist of summoner %s' % (summoner_name))
        summoner = self.api.summoner.by_name(summoner_name = unidecode(summoner_name), region = self.region)
        account_id = summoner["accountId"]
        matchlist = self.api.match.matchlist_by_account(encrypted_account_id = account_id, region = self.region)
        matchlist["extractions"] = {"region": self.region}
        self._store(identifier=summoner_name, entity_type=MATCHLIST_COLLECTION, entity=matchlist, upsert=True)
        self.summoner_names_done.append(summoner_name)
        match_ids = [x['gameId'] for x in matchlist['matches']]
        self.match_ids.extend(match_ids)
        return match_ids

    def crawl_past_matchlist_by_time(self, summoner_name, region, begin_time, end_time, **kwargs):
        """Crawls complete matchlist by going through paginated matchlists of given summoner,
        stores it and saves the matchIds"""

        logger.debug('Getting complete matchlist of summoner %s' % (summoner_name))
        summoner = self.api.summoner.by_name(summoner_name = unidecode(summoner_name), region = region)
        account_id = summoner["accountId"]
        ## Start with empty matchlist
        matchlist = self.api.match.matchlist_by_account(encrypted_account_id=account_id,
                                                begin_time=begin_time,
                                                end_time=end_time,
                                                region=region,
                                                **kwargs)
        matchlist["extractions"] = { "region" : region }
        self._store(identifier=summoner_name, entity_type=MATCHLIST_COLLECTION, entity=matchlist, upsert=True)
        self.summoner_names_done.append(summoner_name)
        match_ids = [x['gameId'] for x in matchlist['matches']]
        self.match_ids.extend(match_ids)
        return match_ids

    def crawl_match(self, match_id):
        """Crawl match with given match_id,
        stores it and saves the matchID"""
        ## Check if match is in database and only crawl if not in database
        match_in_db = self.db_client[MATCH_COLLECTION].find({"_id": match_id})
        if match_in_db.count() == 0:
            logger.debug('Crawling match %s' % (match_id))
            try:
                match = self.api.match.by_id(match_id=match_id, region=self.region)
            except HTTPError as e:
                logger.error(e)
                logger.error("Got an HTTP error (above) while crawling match with matchId %s. Trying again." % (match_id))
                time.sleep(2)
                try:
                    match = self.api.match.by_id(match_id=match_id, region=self.region)
                except HTTPError as e:
                    logger.error(e)
                    logger.error("Failed crawling %s while attempting again. Skipping. Exception above." % (match_id))
                    return
                # Try again
            try:
                match["extractions"] = extract_match_infos(match)
                self._store(identifier=match_id, entity_type=MATCH_COLLECTION, entity=match)
                summoner_names = [x['player']['summonerName'] for x in match['participantIdentities']]
                ## remove summoner ids the crawler has already seen
                new_summoner_names = list(set(summoner_names) - set(self.summoner_names_done))
                self.summoner_names = new_summoner_names + self.summoner_names
            except Exception as e:
                logger.error('Could not find participant data in match with id %s' % (match_id))
                print(e)
        else:
            logger.debug("Skipping match with matchId %s. Already in DB" % (match_id))


class LolCrawler(LolCrawlerBase):
    """Randomly crawls matches starting from seed summoner"""

    def start(self, start_summoner_name):
        """Start infinite crawling loop"""
        logger.info("Start crawling")
        last_summoner_cursor = self.db_client[MATCHLIST_COLLECTION].find({"extractions.region": self.region}).sort("$natural", pymongo.DESCENDING)
        if last_summoner_cursor.count() == 0:
            self.summoner_names = [start_summoner_name]
            logger.info("No summoner ids found in database, starting with seed summoner")
        else:
            for i in range(0, 100):
                try:
                    next_summoner_in_mongo = last_summoner_cursor.next()
                    self.summoner_names += [next_summoner_in_mongo["_id"]]
                except StopIteration:
                    break
            self.summoner_names.append(start_summoner_name)
            logger.info("Starting with latest summoner ids in database")
        while True:
            self.crawl()

    def crawl(self):
        summoner_name = self.summoner_names.pop()
        logger.debug("Crawling summoner {summoner_name}".format(summoner_name=summoner_name))

        match_ids = self.crawl_matchlist(summoner_name)
        ## Crawl all match_ids
        for match_id in match_ids:
            self.crawl_match(match_id)

class TopLolCrawler(LolCrawler):
    """Crawl all matches from all challengers"""

    def crawl(self, region, league):
        self.region = region
        '''Crawl all matches from players in given region, league and season'''
        logger.info('Crawling matches for %s players in %s' % (league, region))
        ## Add ids of solo q top summoners to self.summmone_ids
        self._get_top_summoner_names(region, league)
        ## Get all summoner ids of the league
        logger.info("Crawling matchlists of %i players" % (len(self.summoner_names)))
        ## Get matchlists for all summoners
        self._get_top_summoner_matchlists(region, league)
        logger.info("Crawling %i matches" %(len(self.match_ids)))
        self._get_top_summoners_matches()
        self.flush()
        return None

    def _get_top_summoner_names(self, region, league):
        queue = "RANKED_SOLO_5x5"
        if league == 'challenger':
            league_list = self.api.league.challenger_by_queue(region=region, queue=queue)
        elif league == 'grandmaster':
            league_list = self.api.league.grandmaster_by_queue(region=region, queue=queue)
        elif league == 'master':
            league_list = self.api.league.masters_by_queue(region=region, queue=queue)
        self.summoner_names = [x["summonerName"] for x in league_list["entries"]]
        return None

    def _get_top_summoner_matchlists(self, region, league):
        '''Download and store matchlists for self.summoner_names'''
        for summoner_name in list(set(self.summoner_names)):
            try:
                self.crawl_past_matchlist_by_time(summoner_name=summoner_name,
                                              region=region,
                                              begin_time=self.begin_time,
                                              end_time=self.end_time
                                              )
            except Exception as e:
                logger.error(e)
        return None

    def _get_top_summoners_matches(self):
        for match_id in list(set(self.match_ids)):
            try:
                self.crawl_match(match_id)
            except Exception as e:
                logger.error(e)
        return None

    def start(self,
              begin_time=date.today() - timedelta(7),
              regions=['euw', 'kr', 'na1'],
              end_time = date.today(),
              leagues=['challenger', 'master']
              ):

        self.begin_time = int(begin_time.strftime('%s')) * 1000
        self.end_time = int(end_time.strftime('%s')) * 1000

        begin_time_log = datetime.fromtimestamp(self.begin_time / 1000)
        end_time_log = datetime.fromtimestamp(self.end_time / 1000)

        logger.info('Crawling matches between %s and %s' % (begin_time_log, end_time_log))

        for region, league in itertools.product(regions, leagues):
            self.crawl(region=region, league=league)
        logger.info('Finished crawling')

