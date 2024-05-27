import os
from queue import Queue

from dotenv import load_dotenv
from notion_client import Client as NotionClient
from supabase import Client as SupabaseClient
from supabase import create_client


def count(notion_client, root_block: str):
    block_cnt = 0
    word_cnt = 0
    blocks = Queue()

    root = notion_client.blocks.children.list(block_id=root_block)["results"]
    for block in root:
        blocks.put(block)

    while not blocks.empty():
        block = blocks.get()
        if block["has_children"]:
            # print(f'BlockId({block["type"]}): {block["id"]} has children')
            children = notion_client.blocks.children.list(block_id=block["id"])[
                "results"
            ]
            for child in children:
                # print(f'\tBlockId({block["type"]}): {child["id"]}')
                blocks.put(child)

        if "rich_text" in block[block["type"]]:
            # print(f'BlockId({block["type"]}): {block["id"]} has rich text')
            word_cnt += sum(
                [
                    len(rich_text["plain_text"])
                    for rich_text in block[block["type"]]["rich_text"]
                ]
            )

        if block["type"] == "child_database":
            # print(f'BlockId({block["type"]}): {block["id"]} is a child database')
            child_database = notion_client.databases.query(database_id=block["id"])[
                "results"
            ]
            for page in child_database:
                root = notion_client.blocks.children.list(block_id=page["id"])[
                    "results"
                ]
                for child_block in root:
                    blocks.put(child_block)

        block_cnt += 1
    return block_cnt, word_cnt


def insert(supabase_client: SupabaseClient, plan_id, block_cnt, word_cnt):
    supabase_client.table("statistics").insert(
        {
            "plan_id": plan_id,
            "block_cnt": block_cnt,
            "word_cnt": word_cnt,
        }
    ).execute()


def main():
    load_dotenv()

    supabase_client: SupabaseClient = create_client(
        supabase_url=os.getenv("SUPABASE_URL"),
        supabase_key=os.getenv("SUPABASE_KEY"),
    )
    notion_client = NotionClient(auth=os.getenv("NOTION_AUTH"))

    plans = (supabase_client.table("plans").select("id, root_block").execute()).data

    for plan in plans:
        block_cnt, word_cnt = count(notion_client, plan["root_block"])
        insert(supabase_client, plan["id"], block_cnt, word_cnt)
        print(f"Plan {plan['id']} done. {block_cnt} blocks and {word_cnt} words")


if __name__ == "__main__":
    main()
