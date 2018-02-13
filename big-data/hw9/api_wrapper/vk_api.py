import logging
import requests

logger = logging.getLogger(__name__)


class VkApi(object):
    __base_url = "https://api.vk.com/method/"
    __resolve_screen_name_method = "utils.resolveScreenName"
    __get_user_details = "users.get"
    __wall_get_method = "wall.get"

    def __init__(self, version, token, sleep_time=1):
        self.sleep_time = sleep_time
        self.token = token
        self.version = version

    def __get_base_params(self):
        return {
            "v": self.version,
            "access_token": self.token
        }

    def resolve_screen_name(self, screen_name):
        try:
            logger.info("Access {} method".format(self.__resolve_screen_name_method))
            request_params = self.__get_base_params()
            request_params["screen_name"] = screen_name

            url = "{}{}".format(self.__base_url, self.__resolve_screen_name_method)
            response = requests.post(url, request_params)

            if not response.ok:
                logger.error(response.text)
                return

            return response.json()["response"]
        except Exception as ex:
            logger.exception(ex)

    def get_wall(self, owner_id, count, offset):
        try:
            logger.info("Access {} method for owner {}".format(self.__wall_get_method, owner_id))
            request_params = self.__get_base_params()
            request_params["owner_id"] = owner_id
            request_params["filter"] = "owner"
            request_params["count"] = count
            request_params["offset"] = offset

            url = "{}{}".format(self.__base_url, self.__wall_get_method)
            response = requests.post(url, request_params)
            response_json = response.json()

            if not response.ok or "error" in response_json:
                logger.error(response.text)
                return

            return response_json["response"]
        except Exception as ex:
            logger.exception(ex)

    def get_users(self, ids):
        try:
            logger.info("Access {} method".format(self.__get_user_details))
            request_params = self.__get_base_params()
            request_params["user_ids"] = ",".join(map(str, ids))
            request_params["fields"] = ",".join(["sex", "bdate", "activities", "interests", "music", "movies",
                                                 "tv", "books", "games", "about", "quotes"])

            url = "{}{}".format(self.__base_url, self.__get_user_details)
            response = requests.post(url, request_params)
            response_json = response.json()

            if not response.ok or "error" in response_json:
                logger.error(response.text)
                return

            return response_json["response"]
        except Exception as ex:
            logger.exception(ex)
