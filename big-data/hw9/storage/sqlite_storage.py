import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SqliteStorage(object):

    __insert_user = "INSERT INTO Users ({}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    __insert_post = "INSERT INTO Posts ({}) VALUES (?, ?)"
    __insert_user_id_to_done = "INSERT INTO Done (user_id) VALUES (?)"
    __mark_user_done = "UPDATE Done SET done=1 WHERE user_id=?"
    __get_user = """SELECT u.*, p.posts
                    FROM Users AS u
                    LEFT JOIN (SELECT from_id, group_concat(text) AS posts
                          FROM Posts
                          GROUP BY from_id) AS p
                    ON u.id = p.from_id
                    WHERE u.id = ?"""
    __check_user = "SELECT 1 FROM Users WHERE id = ?"

    def __init__(self, path=None):
        self.path = path if path else "vk.db"

    def get_connection(self, as_dict=False):
        path = Path(self.path)
        if path.exists():
            conn = sqlite3.connect(self.path)
        else:
            conn = sqlite3.connect(self.path)
            conn.execute("""CREATE TABLE IF NOT EXISTS Users (
                            id INTEGER PRIMARY KEY,
                            sex INTEGER,
                            bdate TEXT,
                            activities TEXT,
                            interests TEXT,
                            music TEXT,
                            movies TEXT,
                            tv TEXT,
                            books TEXT,
                            games TEXT,
                            about TEXT,
                            quotes TEXT
                          );""")
            conn.execute("""CREATE TABLE IF NOT EXISTS Posts (
                            id INTEGER PRIMARY KEY,
                            from_id INTEGER NOT NULL,
                            text TEXT,
                            FOREIGN KEY (from_id) REFERENCES Users(id)
                          );""")
            conn.execute("""CREATE TABLE IF NOT EXISTS Done (
                            user_id INTEGER NOT NULL,
                            done INTEGER NOT NULL DEFAULT 0,
                            FOREIGN KEY (user_id) REFERENCES Users(id)
                          );""")
        if as_dict:
            conn.row_factory = sqlite3.Row
        return conn

    def get_current_users_count(self):
        with self.get_connection() as conn:
            return conn.execute("SELECT COUNT(id) FROM Users").fetchone()[0]

    def get_missing_ids_gen(self, stop, start=0):
        ids = (x for x in range(start, stop+1))

        with self.get_connection() as conn:
            db_ids = conn.execute("""SELECT id
                                     FROM Users
                                     WHERE id BETWEEN ? AND ?
                                  """, (start, stop+1))
            while True:
                try:
                    db_id = db_ids.fetchone()
                    range_id = next(ids)
                    while db_id is None or db_id[0] > range_id:
                        yield range_id
                        range_id = next(ids)
                except StopIteration:
                    break

    def get_not_processed_users_gen(self):
        with self.get_connection() as conn:
            conn.executescript("""DROP TABLE IF EXISTS UsersWithNoPosts;
                                  CREATE TEMPORARY TABLE IF NOT EXISTS UsersWithNoPosts AS
                                     SELECT user_id
                                     FROM Done
                                     WHERE done=0
                               """)
            db_ids = conn.execute("SELECT user_id FROM UsersWithNoPosts")

            while True:
                db_id = db_ids.fetchone()
                if db_id is None:
                    break
                else:
                    yield db_id[0]

    def write_users(self, user_infos):
        if not user_infos:
            return
        keys = user_infos[0].keys()
        users = [tuple([x[k] for k in keys]) for x in user_infos]

        with self.get_connection() as conn:
            conn.executemany(self.__insert_user.format(",".join(keys)), users)
            conn.executemany(self.__insert_user_id_to_done, [(x["id"],) for x in user_infos])
            conn.commit()

    def write_posts(self, post_infos):
        if not post_infos:
            return
        keys = post_infos[0].keys()
        posts = [tuple([x[k] for k in keys]) for x in post_infos]

        with self.get_connection() as conn:
            conn.executemany(self.__insert_post.format(",".join(keys)), posts)
            conn.execute(self.__mark_user_done, (post_infos[0]["from_id"],))
            conn.commit()

    def mark_user_as_processed(self, user_id):
        with self.get_connection() as conn:
            conn.execute(self.__mark_user_done, (user_id,))

    def check_user_exists(self, user_id):
        with self.get_connection() as conn:
            return conn.execute(self.__check_user, (user_id,)).fetchone()


    def get_user(self, user_id):
        with self.get_connection(True) as conn:
            cursor = conn.execute(self.__get_user, (user_id,))
            row = cursor.fetchone()
            data = dict(zip([c[0] for c in cursor.description], row))
        return data


