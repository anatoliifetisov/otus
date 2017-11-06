import logging
import requests
import json
import time

from collections import defaultdict
from parsers.markdown_parser import MarkdownParser

from datetime import datetime as dt

logger = logging.getLogger(__name__)

# it has no permissions except default anyway, so I don't care if anyone uses it
token = "0c260e54b80a9e0acaa995a41faf9b75ae7cf85c"

domain = "github.com"
raw_readme_template = "https://raw.githubusercontent.com/{}/{}/master/README.md"
repo_info_call_template = "https://api.github.com/repos/{}/{}"

base_url = raw_readme_template.format("bayandin", "awesome-awesomeness")
headers = {"User-Agent": "awesomeness-scraper"}


class AwesomeScrapper(object):
    """
    Parses links from awesome-awesomeness list on github and retrieves all awesome repos info.
    """
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects
        self.parser = MarkdownParser(None)

    def make_api_call(self, endpoint):
        """
        Performs  a GET request against an endpoint
        :param endpoint: endpoint to GET
        :return: True and response if successful, False and an empty string if not
        """
        response = requests.get(endpoint + "?access_token={}".format(token), headers=headers)
        if response.ok:
            return True, response.text
        elif response.status_code == 403 and "X-RateLimit-Limit" in response.headers:
            _, rate_limit = self.make_api_call("https://api.github.com/rate_limit")
            reset_time = dt.fromtimestamp(int(json.loads(rate_limit)["rate"]["reset"]))
            seconds_until_reset = (reset_time - dt.now()).seconds
            logger.warning("Oops, hourly limit is exhausted, going to sleep for {} seconds".format(seconds_until_reset))
            logger.info("And back to work")
            time.sleep(seconds_until_reset + 10)
            return self.make_api_call(endpoint)
        else:
            logger.error("Error occurred, \"{}\" returned \"{}\"".format(endpoint, response.text))
            return False, ""

    def get_repo_info(self, user, repo_name):
        """
        Retrieves repository info
        :param user: repository's owner
        :param repo_name: repository's name
        :return:
        """
        return self.make_api_call(repo_info_call_template.format(user, repo_name))

    def scrap_process(self, storage):
        data = requests.get(base_url)

        topics = defaultdict(list)
        for pair in self.parser.parse(data, level=0):
            text, link = pair
            if domain in link:
                user, repo = link.split("/")[-2:]
                topics[text].append(raw_readme_template.format(user, repo))

        for topic in topics:
            logger.info("Current topic is {}".format(topic))
            for awesome_repo in topics[topic]:
                data = requests.get(awesome_repo)
                leaf_links = self.parser.parse(data, level=1)
                for pair in leaf_links:
                    _, link = pair
                    link = link.rstrip("/")
                    if domain in link and "gist" not in link and link.count("/") == 4:
                        user, repo = link.split("/")[-2:]
                        success, repo_json = self.get_repo_info(user, repo)
                        if not success:
                            continue
                        repo_dict = json.loads(repo_json)
                        success, commits = self.make_api_call(repo_dict["commits_url"][:-6])
                        if not success:
                            continue
                        success, contributors = self.make_api_call(repo_dict["contributors_url"])
                        if not success:
                            continue
                        storage.append_line("\t".join([topic, repo_json, commits, contributors]))
