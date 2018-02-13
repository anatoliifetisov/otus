import logging

from api_wrapper.vk_api import VkApi
from storage.sqlite_storage import SqliteStorage

logger = logging.getLogger(__name__)


class VkScraper(object):
    __user_fields = {"id", "sex", "bdate", "activities", "interests", "music",
                     "movies", "tv", "books", "games", "about", "quotes"}
    __post_fields = {"from_id", "text"}

    def __init__(self, vk_api: VkApi, storage: SqliteStorage):
        self.storage = storage
        self.vk_api = vk_api

    def scrape_users(self, ids):
        batch = []
        i = 0
        for id in ids:
            batch.append(id)
            i += 1
            if i % 1000 == 0:
                users = self.vk_api.get_users(batch)
                self.storage.write_users(self.__clean_users(users))
                batch, i = [], 0
        if batch:
            users = self.vk_api.get_users(batch)
            self.storage.write_users(self.__clean_users(users))
        logger.info("scraped users: {}".format(ids))

    def scrape_posts(self, user_ids):
        for user in user_ids:
            wall = []
            posts = self.vk_api.get_wall(user, 100, 0)
            if posts is None:
                self.storage.mark_user_as_processed(user)
                continue
            count = posts["count"]
            if count > 100:
                wall += posts["items"]
                offset = 100
                while offset < count:
                    posts = self.vk_api.get_wall(user, 100, offset)
                    if posts is not None:
                        wall += posts["items"]
                        offset += 100
            elif count == 0:
                self.storage.mark_user_as_processed(user)
                continue
            else:
                wall = posts["items"]
            self.storage.write_posts(self.__clean_posts(wall))
            logger.info("scraped posts for user: {}".format(user))

    def __clean_users(self, users):
        for user in users:
            for f in self.__user_fields:
                if f not in user or user[f] == "":
                    user[f] = None
            for k in set(user.keys()):
                if k not in self.__user_fields:
                    del user[k]
        return users

    def __clean_posts(self, posts):
        for post in posts:
            for f in self.__post_fields:
                if f not in post or post[f] == "":
                    post[f] = None
            for k in set(post.keys()):
                if k not in self.__post_fields:
                    del post[k]
        return posts
