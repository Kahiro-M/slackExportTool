import json
import logging
from pathlib import Path
from typing import Any, Literal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import datetime
import shutil

class Client:
    def __init__(self, token) :
        self._token = token
        self._headers = {"Authorization": "Bearer {}".format(token)}
        self._session = requests.Session()
        self._session.mount(
            "https://slack.com/",
            HTTPAdapter(max_retries=Retry(total=5, backoff_factor=3)),
        )

    def _call(self, url, params=None) :
        if not params:
            params = {}

        response = self._session.get(url, headers=self._headers, params=params, timeout=3)
        response.raise_for_status()
        return response.json()

    def fetch_users(self) :
        """ユーザーをすべて取得する
        References:
            - https://api.slack.com/methods/users.list
        """
        response = self._call("https://slack.com/api/users.list")
        return response["members"]

    def fetch_channels(self) :
        """チャンネルをすべて取得する
        References:
            - https://api.slack.com/methods/conversations.list
        """
        response = self._call(
            "https://slack.com/api/conversations.list",
            params={
                # "types": "public_channel,private_channel,mpim,im",
                "types": "public_channel,private_channel",
                "exclude_archived": True,
            },
        )
        return response["channels"]

    def fetch_messages(self, channel_id: str) :
        """指定したチャンネルのメッセージ（スレッドを除く）をすべて取得する
        References:
            - https://api.slack.com/methods/conversations.history
        """
        messages = []
        next_cursor = None
        while True:
            params = {"channel": channel_id, "limit": 200}
            if next_cursor:
                params["cursor"] = next_cursor

            response = self._call("https://slack.com/api/conversations.history", params=params)
            messages += response["messages"]

            if response["has_more"]:
                next_cursor = response["response_metadata"]["next_cursor"]
            else:
                break

        return messages

    def fetch_replies(self, channel_id: str, thread_ts: float) :
        """指定したチャンネル・時刻のスレッド内のメッセージをすべて取得する
        References:
            - https://api.slack.com/methods/conversations.replies
        """
        replies = []
        next_cursor = None
        while True:
            payload = {"channel": channel_id, "limit": 200, "ts": thread_ts}
            if next_cursor:
                payload["cursor"] = next_cursor

            response = self._call("https://slack.com/api/conversations.replies", params=payload)

            done = False
            for message in response["messages"]:
                if message["ts"] == thread_ts and len(replies) > 0:
                    done = True
                    break

                replies.append(message)

            if done:
                break
            elif response["has_more"]:
                next_cursor = response["response_metadata"]["next_cursor"]

        return replies


def main(
    token: str,
    output_dir: Path,
    output_format: Literal["json", "jsonl"] = "jsonl",
) :
    """Slack のメッセージをエクスポートする

    Args:
        token (str): Slack の OAuth トークン
        output_dir (Path): エクスポートしたデータの保存先ディレクトリ
        output_format (Literal["json", "jsonl"], optional): 保存するときのファイル形式
    """
    output_dir.mkdir(exist_ok=True)

    client = Client(token)

    logger.info("Fetching users")
    users = {user["id"]: user for user in client.fetch_users()}
    # users.json(Slack公式エクスポートと同形式)を生成
    output_path = f"{output_dir / 'users'}.{output_format}"
    with open(output_path, "w", encoding='utf-8') as f:
        if output_format == "json":
            json.dump(
                list(users.values()),
                f,
                indent=4,
                ensure_ascii=False,
                sort_keys=True,
            )
        elif output_format == "jsonl":
            for user in users:
                json.dump(user, f, ensure_ascii=False, sort_keys=True)
                f.write("\n")
    logger.info(f"{len(users)} users fetched")

    logger.info("Fetching channels")
    channels = client.fetch_channels()
    # channels.json(Slack公式エクスポートと同形式)を生成
    output_path = f"{output_dir / 'channels'}.{output_format}"
    with open(output_path, "w", encoding='utf-8') as f:
        if output_format == "json":
            json.dump(
                channels,
                f,
                indent=4,
                ensure_ascii=False,
                sort_keys=True,
            )
        elif output_format == "jsonl":
            for channel in channels:
                json.dump(channel, f, ensure_ascii=False, sort_keys=True)
                f.write("\n")
    logger.info(f"{len(channels)} channels fetched")

    now = datetime.datetime.now()
    now_str = now.strftime('%Y-%m-%d_%H%M')


    for channel in channels:
        channel_id = channel["id"]
        channel_name = channel.get("name") or users[channel.get("user")]["name"]

        logger.info(f"Fetching messages: {channel_name=}")
        messages = client.fetch_messages(channel_id)

        messages_and_replies = []
        for message in reversed(messages):
            thread_ts = message.get("thread_ts")
            if not thread_ts:
                messages_and_replies.append(message)
                continue

            replies = client.fetch_replies(channel_id, thread_ts)
            messages_and_replies += replies

        logger.info(f"{len(messages_and_replies)} messages/replies fetched")

        # チャンネル毎にフォルダ作成
        channel_dir = f"{output_dir / channel_name}"
        Path(channel_dir).mkdir(exist_ok=True)
        output_path = f"{output_dir / channel_name / now_str}.{output_format}"
        with open(output_path, "w", encoding='utf-8') as f:
            if output_format == "json":
                json.dump(
                    messages_and_replies,
                    f,
                    indent=4,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            elif output_format == "jsonl":
                for message in messages_and_replies:
                    json.dump(message, f, ensure_ascii=False, sort_keys=True)
                    f.write("\n")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Export Slack history")
    parser.add_argument("--token", required=True, help="OAuth Access Token")
    parser.add_argument("--output-dir", default="output", help="Output Directory")
    parser.add_argument("--output-format", default="json", help="Output Format (json or jsonl)")
    args = parser.parse_args()

    main(args.token, Path(args.output_dir), args.output_format)
    shutil.make_archive(args.output_dir, format='zip', root_dir=args.output_dir)
    zip_path = f'{args.output_dir}.zip'
    if(Path(zip_path).exists()):
        shutil.rmtree(args.output_dir)