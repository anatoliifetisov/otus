import logging
import sys
import json

import pandas as pd
import datetime as dt

from scrappers.awesome_awesomeness_scrapper import AwesomeScrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'


def gather_process():
    logger.info("gather")
    storage = FileStorage(SCRAPPED_FILE)

    scrapper = AwesomeScrapper()
    scrapper.scrap_process(storage)


def convert_data_to_table_format():
    logger.info("transform")
    storage = FileStorage(SCRAPPED_FILE)

    labels = ["topic", "user", "repository", "commits", "contributors", "stars", "forks", "subscribers", "issues",
              "updated"]
    data = []

    for line in storage.read_data():
        split = line.split("\t")
        topic, repo_json, commits_json, contributors_json = split
        repo_dict = json.loads(repo_json)
        commits = len(json.loads(commits_json))
        contributors = len(json.loads(contributors_json))
        name = repo_dict["name"]
        user = repo_dict["owner"]["login"]
        stars = repo_dict["stargazers_count"]
        forks = repo_dict["forks_count"]
        subscribers = repo_dict["subscribers_count"]
        updated = repo_dict["updated_at"]
        issues = repo_dict["open_issues_count"]
        entry = [topic, user, name, commits, contributors, stars, forks, subscribers, issues, updated]
        data.append(entry)
    df = pd.DataFrame.from_records(data, columns=labels)
    df.to_csv(TABLE_FORMAT_FILE)


def stats_of_data():
    logger.info("stats")

    df = pd.read_csv("data.csv", index_col=0,
                     converters={"updated": lambda s: dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")})

    counts = df.groupby("topic").size()
    topic_with_most_repos = counts.idxmax()
    max_count = counts.loc[topic_with_most_repos]
    print("Topic with the most repos is \"{}\". It has {} of them.".format(topic_with_most_repos, max_count))

    most_stars = df.iloc[df.stars.idxmax()]
    print("The mostly starred repo is \"{}\" from the topic \"{}\"".format(most_stars.repository, most_stars.topic))

    repos_with_only_initial_commit = df[df.commits == 1]
    mean_forks = repos_with_only_initial_commit.forks.mean()
    print("There are {} repos with only initial commit. They have {} forks on average"
          .format(len(repos_with_only_initial_commit), round(mean_forks, 2)))

    repos_by_user = df.groupby("user").size()
    most_prolific_user = repos_by_user.idxmax()
    max_repos = repos_by_user.loc[most_prolific_user]
    print("The most awesome user is \"{}\". They have {} awesome repos.".format(most_prolific_user, max_repos))

    users_commits_corr = df.contributors.corr(df.commits)

    def humanize_corr(corr):
        if corr == 0:
            return "no"
        direction = "positive" if corr > 0 else "negative"
        ratio = "slight" if abs(corr) < 0.5 else "strong"
        return "{} {}".format(ratio, direction)

    print("There is a {} correlation ({}) between number of contributors and commits"
          .format(humanize_corr(users_commits_corr), round(users_commits_corr, 2)))

    df["delta"] = dt.datetime.now() - df.updated
    mean_time_since_update = df.delta.mean()
    std_time_since_update = df.delta.std()
    print("Mean time since last update across all repos is {} days with a standard deviation of {} days"
          .format(mean_time_since_update.days, std_time_since_update.days))

    issues_per_topic = df[["topic", "issues"]].groupby("topic").sum().issues
    topic_with_most_issues = issues_per_topic.idxmax()
    max_issues = issues_per_topic.loc[topic_with_most_issues]
    print("Topic \"{}\" has max number of open issues ({})".format(topic_with_most_issues, max_issues))


if __name__ == '__main__':
    logger.info("Work started")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    logger.info("work ended")
