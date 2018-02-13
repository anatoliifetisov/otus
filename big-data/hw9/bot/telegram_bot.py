import datetime
import logging
import dill
import re

import numpy as np
import pandas as pd
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class TelegramBot(object):
    vk_profile_link_validator = re.compile(r"^https?://(www.)?(m.)?(vkontakte.ru|vk.com)/.*$")
    START_COMMAND_TEXT = "Hello! Send a link to a VK user's profile and I'll try to predict their age and sex"
    NOT_A_VK_USER_LINK = "Seems like it is not a VK profile link. Try again, please"
    ERROR = "Ooops! Error occurred, try again later"
    NOT_ENOUGH_DATA = "Unfortunately, there's not enough data to tell anything about this user"
    AGE_ANSWER_TEMPLATE = "It looks like this user is {} years old"
    SEX_ANSWER_TEMPLATE = "It looks like this user is a {}"
    FULL_ANSWER_TEMPLATE = "It looks like this user is a {} years old {}."
    GOT_USER_INFO = "Got user info, analyzing"
    TIME_CONSUMING_OPERATION = "Now I'm gonna think really hard (and long)"
    TIME_CONSUMING_OPERATION_AGAIN = "Give me some more time"
    PROMT = "Try another one"

    date_regex = re.compile("\d{1,2}\.\d{1,2}\.\d{4}")

    def configure(self, vk_api, scraper, storage, token, pipeline_age, age_model, pipeline_sex, sex_model):
        self.vk_api = vk_api
        self.scraper = scraper
        self.storage = storage
        self.token = token
        self.pipeline_age = pipeline_age
        self.age_model = age_model
        self.pipeline_sex = pipeline_sex
        self.sex_model = sex_model

        self.updater = Updater(token=token)
        dispatcher = self.updater.dispatcher
        start_command_handler = CommandHandler("start", self.start_command)
        text_message_handler = MessageHandler(Filters.text, self.text_message)
        dispatcher.add_handler(start_command_handler)
        dispatcher.add_handler(text_message_handler)

    def run(self):
        self.updater.start_polling(clean=True)
        logger.info("started polling")
        self.updater.idle()

    def start_command(self, bot, update):
        bot.send_message(chat_id=update.message.chat_id, text=self.START_COMMAND_TEXT)
        logger.info("started a chat with a user {}".format(update.message.chat.username))

    def text_message(self, bot, update):
        logger.info("received a message: {}".format(update.message.text))

        def check_date(s):
            if s is None:
                return False
            return self.date_regex.match(s)

        def get_age_from_bdate(s):
            if s is None:
                return np.nan

            d = None
            try:
                d = datetime.datetime.strptime(s, "%d.%m.%Y")
            except ValueError:
                if len(s) > 4 and s[:4] == "29.2":  # apparently, VK just can't perform leap years check reliably
                    d = datetime.datetime.strptime("28.2." + s[-4:], "%d.%m.%Y")
            if d is not None:
                return (datetime.datetime.now() - d).days / 365.25
            return None

        def get_age_from_data(data):
            bdate = data["bdate"]
            if check_date(bdate):
                return get_age_from_bdate(bdate)
            return None

        def get_sex_from_data(data):
            sex = data["sex"]
            if sex == 0:
                return None
            return sex

        def get_user(user_id):
            if self.storage.check_user_exists(user_id):
                user = self.storage.get_user(user_id)
            else:
                self.scraper.scrape_users([user_id])
                self.scraper.scrape_posts([user_id])
                user = self.storage.get_user(object_id)
            return user

        def convert_to_df(data):
            index = data["id"]
            del data["id"]
            return pd.DataFrame(data, index=[index])

        def sex_to_string(sex):
            return "male" if sex == 2 else "female"

        age = None
        sex = None

        try:
            entered_text = update.message.text
            if not self.vk_profile_link_validator.match(entered_text):
                answer = self.NOT_A_VK_USER_LINK
            else:
                parse_result = urlparse(entered_text)
                path = parse_result.path.strip('/')
                data = self.vk_api.resolve_screen_name(path)
                if not data:
                    answer = self.NOT_A_VK_USER_LINK
                else:
                    object_id = data["object_id"]
                    if data["type"] == "group":
                        answer = self.NOT_A_VK_USER_LINK
                    else:
                        user = get_user(object_id)
                        logger.info("retrieved user info for user ".format(object_id))
                        bot.send_message(chat_id=update.message.chat_id, text=self.GOT_USER_INFO)
                        user_df = convert_to_df(user)

                        age_from_data = get_age_from_data(user)
                        if age_from_data is not None:
                            age = age_from_data

                        sex_from_data = get_sex_from_data(user)
                        if sex_from_data is not None:
                            sex = sex_from_data

                        del user["bdate"]
                        del user["sex"]

                        if sum(bool(v) for _, v in user.items()) == 0:
                            logger.warning("user {} has no text data".format(object_id))
                            if sex is None and age is None:
                                answer = self.NOT_ENOUGH_DATA
                            elif sex is None and age is not None:
                                answer = self.AGE_ANSWER_TEMPLATE.format(int(age))
                            elif sex is not None and age is None:
                                answer = self.SEX_ANSWER_TEMPLATE.format(sex_to_string(sex))
                            else:
                                answer = self.FULL_ANSWER_TEMPLATE.format(int(age), sex_to_string(sex))
                        else:
                            logger.warning("transforming data")
                            bot.send_message(chat_id=update.message.chat_id, text=self.TIME_CONSUMING_OPERATION)
                            transformed_for_age = self.pipeline_age.transform(user_df)
                            bot.send_message(chat_id=update.message.chat_id, text=self.TIME_CONSUMING_OPERATION_AGAIN)
                            transformed_for_sex = self.pipeline_sex.transform(user_df)

                            if age is None:
                                age = self.age_model.predict(transformed_for_age)
                            else:
                                logger.warning("refitting age model")
                                self.age_model.partial_fit(transformed_for_age, [age])
                                with open("age_model.pkl", "wb") as f:
                                    dill.dump(self.age_model, f)

                            if sex is None:
                                sex = self.sex_model.predict(transformed_for_sex) + 1
                            else:
                                logger.warning("refitting sex model")
                                self.sex_model.partial_fit(transformed_for_sex, [sex - 1])
                                with open("sex_model.pkl", "wb") as f:
                                    dill.dump(self.sex_model, f)

                            answer = self.FULL_ANSWER_TEMPLATE.format(int(age), sex_to_string(sex))

            bot.send_message(chat_id=update.message.chat_id, text=answer)
            bot.send_message(chat_id=update.message.chat_id, text=self.PROMT)
        except Exception as e:
            logging.error(e)
            bot.send_message(chat_id=update.message.chat_id, text=self.ERROR)
