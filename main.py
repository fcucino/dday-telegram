import logging
import os
import re
import sys
import time
import string
from dataclasses import dataclass
from difflib import ndiff
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
TELEGRAM_CHANNEL = '@dday_it_feed'
TELEGRAM_LOGS_CHANNEL = os.environ['TG_LOGS_CHANNEL_ID']

UA = 'DDay.it News Telegram (+https://github.com/turbostar190/dday-telegram)'
feedparser.USER_AGENT = UA

DATABASE_PATH = os.environ.get('DATABASE_PATH', 'dday.db')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S%z', stream=sys.stdout)
logger = logging.getLogger(__name__)

logger.info('Database path: ' + DATABASE_PATH)

db = SqliteDatabase(DATABASE_PATH)


class Article(Model):
    title = TextField()
    description = TextField()
    link = TextField()
    image = TextField()
    published = IntegerField()
    updated = IntegerField()
    telegram_message_id = IntegerField(null=True, unique=True)

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

    feed = feedparser.parse('https://www.dday.it/rss?_=' + str(int(time.time())))

    if Article.select().count() == 0:
        first_run(feed)
        return

    for entry in reversed(feed.entries):
        article = Article.get_or_none(link=parse_url(entry))

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
        image=entry.links[1].href,
        tags=details['tags'],
    )

    # Article already exists
    if article := Article.get_or_none(link=parse_url(entry)):
        article: Article

        logger.info(f'Updating article: {parse_url(entry)} (old: {article.link})')
        if not article.telegram_message_id:
            logger.warning('Article has no telegram_message_id, skipping')
            # fix for articles added on the first run and never sent
            article.updated = int(time.mktime(entry.updated_parsed))
            article.save()
            return
        
        try:
            send_message(message, article.telegram_message_id, entry.updated)
        except RequestException:
            logger.exception('Error updating message')
            return  # so that it's retried later
        
        # send_log(article, entry)
        article.title = entry.title
        article.link = parse_url(entry)
        article.updated = int(time.mktime(entry.updated_parsed))
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
        link + '?_=' + str(int(time.time())),  # fix for 404 ending up in the dolomiti cache
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
        tags = [re.sub(r"\s+", "", list(tag.strings)[0], flags=re.UNICODE).replace("-", "") for tag in tags] # remove spaces and dashes

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
        # msg += '— '

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
            'photo': message.image,
        }
        resp = requests.post(f'{TELEGRAM_API_URL}/sendPhoto',
                             data=payload,
        )

    # Error while editing
    if resp.status_code != 200 and telegram_message_id:
        # Log but don't raise (ignore error)
        logger.error(f'Error editing message: {resp.text}')
        return telegram_message_id
    # Error while sending
    elif resp.status_code != 200:
        logger.error(f'Error sending message: {resp.text}')
        resp.raise_for_status()

    return resp.json()['result']['message_id']


def send_log(article: Article, entry):
    try:
        diff = get_diff(
            telegram_escape(article.title),
            telegram_escape(entry.title)
        )

        requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage', json={
            'chat_id': TELEGRAM_LOGS_CHANNEL,
            'text': f'{diff[0]}\n\n'
                    f'{diff[1]}\n\n'
                    f'<code>{telegram_escape(article.link)}</code>\n\n'
                    f'<code>{telegram_escape(parse_url(entry))}</code>\n\n'
                    f'Message ID: <code>{article.telegram_message_id}</code>',
            'parse_mode': 'HTML',
        })
    except (Exception,):
        logger.exception('Error sending log')


def get_diff(old: str, new: str) -> list:
    removed_from_old = get_diff_removals(old, new)
    removed_from_new = get_diff_removals(new, old)

    offset = 0
    for group in removed_from_old:
        start = group[0] + offset
        end = group[-1] + 1 + offset
        old = old[:start] + '<b><u>' + old[start:end] + '</u></b>' + old[end:]
        offset += len('<u></u><b></b>')

    offset = 0
    for group in removed_from_new:
        start = group[0] + offset
        end = group[-1] + 1 + offset
        new = new[:start] + '<b><u>' + new[start:end] + '</u></b>' + new[end:]
        offset += len('<u></u><b></b>')

    return [old, new]


def get_diff_removals(first: str, second: str) -> list:
    diff = ndiff(first, second)

    removed = []
    offset = 0

    for i, s in enumerate(diff):
        if s[0] == ' ':
            continue
        elif s[0] == '+':
            offset -= 1
        elif s[0] == '-':
            removed.append(i + offset)

    groups = []
    group = []
    for i in range(len(removed)):
        if i == 0:
            group.append(removed[i])
        elif removed[i] - removed[i - 1] == 1:
            group.append(removed[i])
        else:
            groups.append(group)
            group = [removed[i]]

    if group:
        groups.append(group)

    return groups


def telegram_escape(text: str) -> str:
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

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
