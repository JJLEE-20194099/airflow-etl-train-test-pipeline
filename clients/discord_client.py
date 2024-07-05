from discordwebhook import Discord
from discord_webhook import DiscordWebhook, DiscordEmbed

import os
from dotenv import load_dotenv
from datetime import datetime
load_dotenv(override=True)


class DiscordClient:
    def __init__(self, url = None, bot_name = None) -> None:
        self.avatar_url="https://hackmd.io/_uploads/ryCoI5zDC.png"
        self.sucess_icon="https://cdn.discordapp.com/attachments/1089021958429085767/1196355828685475871/success.512x512.png?ex=65b75430&is=65a4df30&hm=9367d7ae96c4463e0eb5d34d4207fc91d4c37182ff25f33362ba5a21b33794d4&"
        self.error_icon="https://cdn.discordapp.com/attachments/1089021958429085767/1196355828907778078/failure.512x512.png?ex=65b75430&is=65a4df30&hm=c266fabd801cff352c2b4fb46b53b3f37c802aedbbaac6c1ec02908d0320d532&"
        self.bot_name=bot_name
        self.author_url = "https://avatars.githubusercontent.com/u/109773340?s=200&v=4"
        self.icon_url = "https://avatars0.githubusercontent.com/u/14542790"
        self.footer_content = "Copyright belongs to Long"
        if url:
            self.webhook_url=url
        else:
            self.webhook_url=os.getenv("HEALTHCHECK_WEBHOOK_URL")


    def generate_noti_message(self, message):
        now = datetime.now()
        message =f"""
        Time : {now}
        ------------
        Message : {message}
        """
        return message


    def webhook_push_sucess_noti(self, title, message):
        status = "Success"
        discord = Discord(url=self.webhook_url)
        discord.post(
            username=self.bot_name,
            avatar_url=self.avatar_url,
            embeds=[
                {
                    "author": {
                        "name" : status,
                        "icon_url": self.sucess_icon
                    },
                    "title": title,
                    "description": self.generate_noti_message(message=message),
                }
            ],
        )

    def webhook_push_error_noti(self, title, message):
        status = "Error"
        discord = Discord(url=self.webhook_url)
        discord.post(
            username=self.bot_name,
            avatar_url=self.avatar_url,
            embeds=[
                {
                    "author": {
                        "name" :  status,
                        "icon_url": self.error_icon
                    },
                    "title": title,
                    "description": self.generate_noti_message(message=message),
                }
            ],
        )