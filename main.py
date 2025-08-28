import logging
import os
import re
import sys
import time
import html
from dataclasses import dataclass
from typing import cast
from hashlib import md5
from typing import Optional

import feedparser
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from peewee import SqliteDatabase, Model, TextField, IntegerField
from requests import RequestException
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from urllib.parse import urlparse

BOT_TOKEN = os.environ['BOT_TOKEN']
TELEGRAM_API_URL = f'https://api.telegram.org/bot{BOT_TOKEN}'
TELEGRAM_CHANNEL = os.environ.get('TELEGRAM_CHANNEL', '@dday_it_feed')

UA = 'DDay.it News Telegram (+https://github.com/turbostar190/dday-telegram)'
feedparser.USER_AGENT = UA

DATABASE_PATH = os.environ.get('DATABASE_PATH', 'dday.db')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S%z', stream=sys.stdout)
logger = logging.getLogger(__name__)

logger.info('Database path: ' + DATABASE_PATH)

db = SqliteDatabase(DATABASE_PATH)


class Article(Model):
    id = IntegerField(primary_key=True)
    title = TextField()
    description = TextField()
    link = TextField()
    image = TextField()
    published = IntegerField()
    updated = IntegerField()
    telegram_message_id = cast(Optional[str], IntegerField(null=True, unique=True))

    class Meta:
        database = db


@dataclass
class TelegramMessage:
    title: str
    link: str
    tags: list
    description: str
    image: str


def parse_url(entry) -> str:
    url = entry.links[0].href
    
    parsed = urlparse(url)
    if ("www" not in parsed.netloc):
        parsed = parsed._replace(netloc="www." + parsed.netloc)
    return parsed.geturl()


def check():
    logger.info('Checking...')

    latest_article = Article.select().order_by(Article.updated.desc()).first()
    last_updated = time.gmtime(latest_article.updated) if latest_article else None

    feed = feedparser.parse('https://www.dday.it/rss', modified=last_updated)
    logger.info(f'Feed {len(feed.entries)} entries, version {feed.version}, status {feed.status}, bozo {feed.bozo}')
    if feed.status == 304:
        logger.debug('Feed not modified')
        return
    if feed.bozo:
        logger.exception('Error parsing feed: %s', str(feed.bozo_exception))
        sys.exit(1)
        return

    if Article.select().count() == 0:
        logger.debug('No articles in database, running first run')
        first_run(feed)
        return

    for entry in reversed(feed.entries):
        parsed_url = parse_url(entry)
        article = Article.get_or_none(link=parsed_url)
        logger.debug(f'Checking article: {parsed_url}')

        if (not article): # or (int(time.mktime(entry.updated_parsed)) > article.updated) # skip updated check for now
            process_new_article(entry)

    logger.info('Done!')


def first_run(feed):
    logger.info('First run, populating database...')
    for entry in reversed(feed.entries):
        article = Article(
            post_id=None,
            title=entry.title.strip(),
            description=strip_description(entry.summary),
            link=parse_url(entry),
            image=entry.links[1].href,
            published=int(time.mktime(entry.published_parsed)),
            updated=int(time.mktime(entry.updated_parsed)),
            telegram_message_id=None
        )
        article.save()
    logger.info('Done!')


def process_new_article(entry):
    details = fetch_article_details(parse_url(entry))
    
    message = TelegramMessage(
        title=entry.title.strip(),
        description=strip_description(entry.summary),
        link=parse_url(entry),
        # image=html.unescape(entry.links[1].href),
        image=download_image(html.unescape(entry.links[1].href)) or "",
        tags=details['tags'],
    )

    # Article already exists
    if article := Article.get_or_none(link=parse_url(entry)):
        article: Article

        logger.info(f'Updating article: {parse_url(entry)} (old: {article.link})')
        if not article.telegram_message_id:
            logger.warning('Article has no telegram_message_id, skipping')
            # fix for articles added on the first run and never sent
            article.__setattr__('updated', int(time.mktime(entry.updated_parsed)))
            article.save()
            return
        
        try:
            send_message(message, article.telegram_message_id, entry.updated)
        except RequestException:
            logger.exception('Error updating message')
            return  # so that it's retried later
        
        article.title = entry.title
        article.__setattr__('link', parse_url(entry))
        article.__setattr__('updated', int(time.mktime(entry.updated_parsed)))
        article.save()

    # Otherwise assume that it's new
    else:
        logger.info(f'Sending article: {message.link}')
        try:
            message_id = send_message(message)
        except RequestException:
            logger.exception('Error sending message')
            return  # so that it's retried later
        Article.create(
            title=message.title,
            description=message.description,
            link=message.link,
            image=message.image,
            published=time.mktime(entry.published_parsed),
            updated=time.mktime(entry.updated_parsed),
            telegram_message_id=message_id
        )


def fetch_article_details(link: str) -> dict:
    resp = requests.get(
        link,
        headers={
            'User-Agent': UA
        },
        timeout=10
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')

    tags = []

    categories = soup.select_one('section.article-category-tags')
    if categories:
        tags = categories.find_all('a', class_='category-tag')
    else:
        # dmove
        pass # doesn't work because of the "no js" overlay
        categories = soup.select_one('div.tags')
        if categories:
            tags = categories.find_all('span', class_='tag')

    if len(tags) > 0:
        tags = [re.sub(r"\s+", "", tag.get_text(), flags=re.UNICODE).replace("-", "") for tag in tags] # remove spaces and dashes

    return {
        'tags': tags,
    }


def download_image(image_url: str) -> Optional[str]:
    if not image_url:
        return None
    try:
        session = requests.Session()
        retries = Retry(total=2, status_forcelist=[502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        resp = session.get(image_url, timeout=10)
        resp.raise_for_status()
        filename = 'images/' + md5(image_url.encode('utf-8')).hexdigest()
        with open(filename, 'wb') as f:
            f.write(resp.content)
        session.close()
        return filename
    except (Exception,):
        logger.exception('Error downloading image')
        return None


def send_message(message: TelegramMessage, telegram_message_id=None, updated_time=None) -> int:
    msg = ''

    msg += f'<strong>{telegram_escape(message.title)}</strong>'
    
    if message.tags:
        msg += '\n'
        for tag in message.tags:
            msg += f'#{tag} '

    if message.description:
        msg += f'\n\n<i>{telegram_escape(message.description)}</i>'

    msg += f'\n\n📰 <a href="{message.link}">Leggi articolo</a>'

    if telegram_message_id and updated_time:
        # msg += f'\n\n<i>EDIT: {time.strftime("%d/%m/%Y %H:%M", updated_time)}</i>'
        msg += f'\n\n<i>EDIT: {updated_time}</i>'

        payload = {
            'chat_id': TELEGRAM_CHANNEL,
            'message_id': telegram_message_id,
            'caption': msg,
            'parse_mode': 'HTML',
        }
        resp = requests.post(f'{TELEGRAM_API_URL}/editMessageCaption', json=payload)
    else:
        payload = {
            'chat_id': TELEGRAM_CHANNEL,
            'caption': msg,
            'parse_mode': 'HTML',
            # 'photo': message.image,
        }
        resp = requests.post(f'{TELEGRAM_API_URL}/sendPhoto',
                             data=payload,
                             files={
                                 'photo': open(message.image, 'rb')
                             })

    # Error while editing
    if resp.status_code != 200 and telegram_message_id:
        # Log but don't raise (ignore error)
        logger.error(f'Error editing message: {resp.text}')
        return telegram_message_id
    # Error while sending
    elif resp.status_code != 200:
        logger.error(f'Error sending message: {resp.text}')
        resp.raise_for_status()
    
    message_id = resp.json()['result']['message_id']
    logger.info(f'Message sent id:{message_id}')

    return message_id

def telegram_escape(text: str) -> str:
    return html.escape(text)
    # return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def strip_description(description: str) -> str:
    description = re.sub(r'<img.*/>', '', description)
    description = re.sub(r'<a.*/a>', '', description)
    description = re.sub(r'\s+', ' ', description)
    return description

def clean():
    logger.info('Cleaning old articles')
    # Keep the last 200 articles
    Article.delete().where(Article.id.not_in(
        Article.select(Article.id).order_by(Article.id.desc()).limit(200)
    )).execute()

    logger.info('Cleaning old images')
    for filename in os.listdir('images'):
        os.remove(os.path.join('images', filename))


if __name__ == '__main__':
    db.create_tables([Article])
    os.makedirs('images', exist_ok=True)

    clean()
    check()

    scheduler = BlockingScheduler()
    scheduler.add_job(check, trigger=CronTrigger(minute='*/9'))
    scheduler.add_job(clean, trigger=CronTrigger(minute='5', hour='1'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
