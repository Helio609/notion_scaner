from notion_client import Client as NotionClient
from supabase import create_client, Client as Supabase
from os import getenv
from dotenv import load_dotenv
from enum import Enum
from queue import Queue


class DetectedType(Enum):
    DATABASE = 1
    PAGE = 2
    NONE = 3


class Scaner:
    def __init__(self, auth: str, supabase_url: str, supabase_key: str) -> None:
        self.__notion_client: NotionClient = NotionClient(auth=auth)
        self.__supabase: Supabase = create_client(supabase_url, supabase_key)

        self.__plans = (
            self.__supabase.table("plans").select("id, root_block").execute().data
        )

    def __insert(self, plan_id: str, block_cnt: int, word_cnt: int):
        self.__supabase.table("statistics").insert(
            {
                "plan_id": plan_id,
                "block_cnt": block_cnt,
                "word_cnt": word_cnt,
            }
        ).execute()

    def __dectect(self, id: str):
        block = self.__notion_client.blocks.retrieve(id)

        if block["type"] == "child_database":
            return DetectedType.DATABASE
        elif block["type"] == "child_page":
            return DetectedType.PAGE

        return None

    def __process_database(self, database_id: str):
        block_cnt = 0
        word_cnt = 0
        has_more = True
        next_cursor = None
        while has_more:
            database = self.__notion_client.databases.query(
                database_id, next_cursor=next_cursor, page_size=100
            )
            if database["has_more"]:
                next_cursor = database["next_cursor"]
            else:
                has_more = False
            for page in database["results"]:
                b_cnt, w_cnt = self.__process_page(page["id"])
                block_cnt += b_cnt
                word_cnt += w_cnt
        return block_cnt, word_cnt

    def __process_page(self, block_id: str):
        block_cnt = 0
        word_cnt = 0
        q = Queue()
        has_more = True
        next_cursor = None

        while has_more:
            blocks = self.__notion_client.blocks.children.list(
                block_id=block_id, next_cursor=next_cursor, page_size=100
            )
            for block in blocks["results"]:
                q.put(block)
            if blocks["has_more"]:
                next_cursor = blocks["next_cursor"]
            else:
                has_more = False

        while not q.empty():
            block = q.get()

            if block["has_children"]:
                print(f'BlockId({block["type"]}): {block["id"]} has children')
                has_more = True
                next_cursor = None
                while has_more:
                    children = self.__notion_client.blocks.children.list(
                        block_id=block["id"],
                        start_cursor=next_cursor,
                        page_size=100,
                    )
                    for child in children["results"]:
                        q.put(child)
                    if children["has_more"]:
                        next_cursor = children["next_cursor"]
                    else:
                        has_more = False

            if "rich_text" in block[block["type"]]:
                print(f'BlockId({block["type"]}): {block["id"]} has rich text')
                word_cnt += sum(
                    [
                        len(rich_text["plain_text"])
                        for rich_text in block[block["type"]]["rich_text"]
                    ]
                )

            if block["type"] == "child_database":
                print(f'BlockId({block["type"]}): {block["id"]} is a child database')
                b_cnt, w_cnt = self.__process_database(block["id"])
                block_cnt += b_cnt
                word_cnt += w_cnt
                continue

            block_cnt += 1

        return block_cnt, word_cnt

    def __process(self, id: str):
        block_cnt = 0
        word_cnt = 0
        detectedType = self.__dectect(id)
        if detectedType == DetectedType.DATABASE:
            block_cnt, word_cnt = self.__process_database(database_id=id)
        elif detectedType == DetectedType.PAGE:
            block_cnt, word_cnt = self.__process_page(block_id=id)
        else:
            print("Nothing to do.")
        return block_cnt, word_cnt

    def __run(self, id: str):
        return self.__process(id)

    def run(self):
        for plan in self.__plans:
            plan_id, id = plan["id"], plan["root_block"]
            block_cnt, word_cnt = self.__run(id)
            self.__insert(plan_id, block_cnt, word_cnt)
            print(f"Plan {plan_id} done, {block_cnt} blocks and {word_cnt} words.")


if __name__ == "__main__":
    load_dotenv()

    NOTION_AUTH = getenv("NOTION_AUTH")
    SUPABASE_URL = getenv("SUPABASE_URL")
    SUPABASE_KEY = getenv("SUPABASE_KEY")

    if not NOTION_AUTH or not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Please provide env vars")

    scaner = Scaner(NOTION_AUTH, SUPABASE_URL, SUPABASE_KEY)
    scaner.run()
