import functools
from enum import Enum
from os import getenv
from queue import Queue
from random import shuffle
from time import sleep

from dotenv import load_dotenv
from notion_client import APIResponseError
from notion_client import Client as NotionClient
from notion_client.errors import APIErrorCode
from supabase import Client as Supabase
from supabase import create_client


class DetectedType(Enum):
    DATABASE = 1
    PAGE = 2
    NONE = 3


def sleep_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sleep(0.35)
        return func(*args, **kwargs)

    return wrapper


class Scaner:
    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        self.__notion_client: NotionClient = None
        self.__supabase: Supabase = create_client(supabase_url, supabase_key)

        self.__plans = Queue()

        plans = (
            self.__supabase.table("plans")
            .select("id, root_id, notion_auth")
            .execute()
            .data
        )

        shuffle(plans)

        for plan in plans:
            self.__plans.put(plan)

    def __last_record(self, plan_id: str):
        last_record = (
            self.__supabase.table("statistics")
            .select("blocks, words")
            .eq("plan_id", plan_id)
            .order("created_at", desc=True)
            .limit(1)
            .maybe_single()
            .execute()
        )

        if last_record:
            return last_record.data["blocks"], last_record.data["words"]

        return None

    def __insert(self, plan_id: str, block_cnt: int, word_cnt: int):
        self.__supabase.table("statistics").insert(
            {
                "plan_id": plan_id,
                "blocks": block_cnt,
                "words": word_cnt,
            }
        ).execute()

    def __update_last_error(self, plan_id: str, error: str | None):
        self.__supabase.table("plans").update({"last_error": error}).eq(
            "id", plan_id
        ).execute()

    @sleep_decorator
    def __retrieve_block(self, id: str):
        return self.__notion_client.blocks.retrieve(id)

    @sleep_decorator
    def __list_block_children(
        self, block_id: str, next_cursor: str = None, page_size: int = 100
    ):
        return self.__notion_client.blocks.children.list(
            block_id=block_id, next_cursor=next_cursor, page_size=page_size
        )

    @sleep_decorator
    def __query_database(
        self, database_id: str, next_cursor: str = None, page_size: int = 100
    ):
        return self.__notion_client.databases.query(
            database_id=database_id, start_cursor=next_cursor, page_size=page_size
        )

    def __dectect(self, id: str):
        block = self.__retrieve_block(id)

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
        try:
            while has_more:
                # print(f"Database {database_id} has more")
                database = self.__query_database(database_id, next_cursor=next_cursor)
                if database["has_more"]:
                    next_cursor = database["next_cursor"]
                else:
                    has_more = False
                for page in database["results"]:
                    b_cnt, w_cnt = self.__process_page(page["id"])
                    block_cnt += b_cnt
                    word_cnt += w_cnt
        except Exception as e:
            print(f"Database error: {e}") # TODO: Just skip it
        return block_cnt, word_cnt

    def __process_page(self, block_id: str):
        block_cnt = 0
        word_cnt = 0
        q = Queue()
        has_more = True
        next_cursor = None

        while has_more:
            # print(f"Block {block_id} has more")
            blocks = self.__list_block_children(block_id, next_cursor=next_cursor)
            for block in blocks["results"]:
                q.put(block)
            if blocks["has_more"]:
                next_cursor = blocks["next_cursor"]
            else:
                has_more = False

        while not q.empty():
            block = q.get()

            if block["has_children"]:
                # print(f'{block["type"]}: {block["id"]} has children')
                has_more = True
                next_cursor = None
                while has_more:
                    children = self.__list_block_children(
                        block_id=block["id"],
                        next_cursor=next_cursor,
                    )
                    for child in children["results"]:
                        q.put(child)
                    if children["has_more"]:
                        next_cursor = children["next_cursor"]
                    else:
                        has_more = False

            if "rich_text" in block[block["type"]]:
                # print(f'{block["type"]} {block["id"]} has rich text')
                word_cnt += sum(
                    [
                        len(rich_text["plain_text"])
                        for rich_text in block[block["type"]]["rich_text"]
                    ]
                )

            if block["type"] == "child_database":
                # print(f'{block["type"]} {block["id"]} is a child database')
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

    def run(self, insert=True):
        while not self.__plans.empty():
            plan = self.__plans.get()
            plan_id, id, notion_auth = plan["id"], plan["root_id"], plan["notion_auth"]
            print(f'Plan {plan_id} started')

            try:
                # Init notion client here
                self.__notion_client = NotionClient(auth=notion_auth)

                block_cnt, word_cnt = self.__run(id)

                last_record = self.__last_record(plan_id)

                if not last_record or (block_cnt, word_cnt) != last_record:
                    if insert:
                        self.__insert(plan_id, block_cnt, word_cnt)

                print(f"Plan {plan_id} done.")
                self.__update_last_error(plan_id, None)
            except Exception as e:
                if isinstance(e, APIResponseError):
                    if (
                        e.code == APIErrorCode.ObjectNotFound
                        or e.code == APIErrorCode.Unauthorized
                    ):
                        print("Notion client fatal: ", end="")
                        print(e)
                        self.__update_last_error(plan_id, str(e))
                        continue

                if "retry" in plan and plan["retry"] == 3:
                    print("Retry fatal: ", end="")
                    print(e)
                    self.__update_last_error(plan_id, str(e))
                    continue

                retry = plan["retry"] + 1 if "retry" in plan else 1
                self.__plans.put(plan | {"retry": retry})
                print(f"Retry({id} {retry}): ", end="")
                print(e)
                sleep(10)


if __name__ == "__main__":
    load_dotenv()

    SUPABASE_URL = getenv("SUPABASE_URL")
    SUPABASE_KEY = getenv("SUPABASE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Please provide env vars")

    scaner = Scaner(SUPABASE_URL, SUPABASE_KEY)
    scaner.run()
