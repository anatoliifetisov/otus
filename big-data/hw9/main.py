import configparser
import logging
import sys
import dill

from api_wrapper.vk_api import VkApi
from bot.telegram_bot import TelegramBot
from scraper.scraper import VkScraper
from storage.sqlite_storage import SqliteStorage

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)


def scrape(vk_api, storage, ids):
    vk_scraper = VkScraper(vk_api, storage)
    vk_scraper.scrape_users(ids)
    vk_scraper.scrape_posts(storage.get_not_processed_users_gen())


def run_bot(vk_api, telegram_token, storage):
    with open("pipeline_age.pkl", "rb") as f:
        pipeline_age = dill.load(f)
    with open("pipeline_sex.pkl", "rb") as f:
        pipeline_sex = dill.load(f)
    with open("age_model.pkl", "rb") as f:
        age_model = dill.load(f)
    with open("sex_model.pkl", "rb") as f:
        sex_model = dill.load(f)

    scraper = VkScraper(vk_api, storage)
    bot = TelegramBot()
    bot.configure(vk_api, scraper, storage, telegram_token, pipeline_age, age_model, pipeline_sex, sex_model)
    bot.run()


def stop_bot():
    pass


def main(args):
    config = configparser.ConfigParser()
    config.read("tokens.cfg")
    vk_api_v = config.get("VkApi", "v")
    vk_api_token = config.get("VkApi", "token")
    vk_api = VkApi(vk_api_v, vk_api_token)
    storage = SqliteStorage("vk.db")
    if args[0] == "scrape":
        scrape(vk_api, storage, range(25000))
    elif args[0] == "start":
        telegram_token = config.get("TeleApi", "token")
        run_bot(vk_api, telegram_token, storage)
    elif args[0] == "stop":
        stop_bot()


if __name__ == "__main__":
    main(sys.argv[1:])
