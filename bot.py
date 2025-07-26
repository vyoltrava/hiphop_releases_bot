# bot.py
import asyncio
import random
import aiohttp
from bs4 import BeautifulSoup
from telegram import Bot
import logging
from datetime import datetime
import os
import sqlite3
from urllib.parse import urljoin
from dotenv import load_dotenv
import schedule
import time
import pytz

# --- Настройка ---
load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
NEWS_CHANNEL = os.getenv('NEWS_CHANNEL_ID') or os.getenv('CHANNEL_ID')
RELEASES_CHANNEL = os.getenv('RELEASES_CHANNEL_ID') or NEWS_CHANNEL

# Определяем Вашингтонскую временную зону (ET)
WASHINGTON_TZ = pytz.timezone('US/Eastern')
UTC_TZ = pytz.utc

# User-Agent Rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
]

# Источники новостей
NEWS_SOURCES = [
    {
        "name": "HipHopDX News",
        "url": "https://hiphopdx.com/news",
        "item_selector": "article, .category-list-item, .list-item",
        "title_selector": "h3 a, .title-link, h2 a",
        "link_selector": "h3 a, .title-link, h2 a",
        "description_selector": ".excerpt, .summary, .description",
        "image_selector": "img[data-src], img[src], .image img",
        "base_url": "https://hiphopdx.com"
    },
    {
        "name": "AllHipHop News",
        "url": "https://www.allhiphop.com/news/",
        "item_selector": ".article, .post-item, .news-card",
        "title_selector": "h2 a, .entry-title a, h3 a",
        "link_selector": "h2 a, .entry-title a, h3 a",
        "description_selector": ".excerpt, .entry-summary, .post-excerpt",
        "image_selector": "img[data-src], img[src], .post-image img",
        "base_url": "https://www.allhiphop.com"
    },
    {
        "name": "XXL News",
        "url": "https://xxl.com/news",
        "item_selector": ".article, .story, .post, .news-item",
        "title_selector": "h2 a, .headline a, h3 a",
        "link_selector": "h2 a, .headline a, h3 a",
        "description_selector": ".excerpt, .summary, .dek",
        "image_selector": "img[data-src], img[src], .media img",
        "base_url": "https://xxl.com"
    }
]

# Источники релизов
# - Источники релизов (обновлённые и улучшенные) -
RELEASE_SOURCES = [
    {
        "name": "HipHopDX Album Reviews",
        "url": "https://hiphopdx.com/album-reviews",
        "item_selector": "article, .category-list-item, .list-item, .review-item",
        "title_selector": "h3 a, .title-link, h2 a",
        "link_selector": "h3 a, .title-link, h2 a",
        "description_selector": ".excerpt, .summary, .description",
        "image_selector": "img[data-src], img[src], .image img",
        "base_url": "https://hiphopdx.com"
    },
    {
        "name": "AllHipHop Album Reviews",
        "url": "https://www.allhiphop.com/tag/album-review/",
        "item_selector": ".article, .post-item, .news-card, .review-card",
        "title_selector": "h2 a, .entry-title a, h3 a",
        "link_selector": "h2 a, .entry-title a, h3 a",
        "description_selector": ".excerpt, .entry-summary, .post-excerpt, .review-excerpt",
        "image_selector": "img[data-src], img[src], .post-image img",
        "base_url": "https://www.allhiphop.com"
    },
    {
        "name": "Billboard Hot 100",
        "url": "https://www.billboard.com/charts/hot-100/",
        "item_selector": ".o-chart-results-list-row",
        "title_selector": "h3 a",
        "link_selector": "h3 a",
        "description_selector": "", # Чарты редко имеют описание, но можно добавить позже
        "image_selector": "img[data-lazy-src], img[src]",
        "base_url": "https://www.billboard.com"
    },
    {
        "name": "Billboard 200 Albums",
        "url": "https://www.billboard.com/charts/billboard-200/",
        "item_selector": ".o-chart-results-list-row",
        "title_selector": "h3 a",
        "link_selector": "h3 a",
        "description_selector": "", # То же самое
        "image_selector": "img[data-lazy-src], img[src]",
        "base_url": "https://www.billboard.com"
    },
    {
        "name": "XXL Freshmen",
        "url": "https://xxl.com/freshmen",
        "item_selector": ".article, .story, .post, .freshmen-item",
        "title_selector": "h2 a, .headline a, h3 a",
        "link_selector": "h2 a, .headline a, h3 a",
        "description_selector": ".excerpt, .summary, .dek",
        "image_selector": "img[data-src], img[src], .media img",
        "base_url": "https://xxl.com"
    }
]

# --- Логика бота ---

class HipHopETBot:
    def __init__(self, token):
        self.bot = Bot(token=token)
        self.db_path = 'posted.db'
        self.init_db()
        logging.basicConfig(level=logging.INFO)
        # Ограничение на 3 одновременных соединения
        self.semaphore = asyncio.Semaphore(3)

    def init_db(self):
        """Создание базы данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posted_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL, -- 'news' или 'release'
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def is_posted(self, url, item_type):
        """Проверка, был ли пост уже опубликован."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM posted_items WHERE url = ? AND type = ?", (url, item_type))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def mark_as_posted(self, url, item_type):
        """Отметить пост как опубликованный."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO posted_items (url, type) VALUES (?, ?)", (url, item_type))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()

    async def fetch_url(self, session, url, retries=3):
        """Асинхронная загрузка URL с повторными попытками."""
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        for attempt in range(retries):
            try:
                async with self.semaphore: # Ограничиваем количество одновременных запросов
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
                        if response.status == 200:
                            return await response.text()
                        else:
                            logging.warning(f"HTTP {response.status} for {url}")
                            if attempt < retries - 1:
                                await asyncio.sleep(2 ** attempt) # Экспоненциальная задержка
            except asyncio.TimeoutError:
                logging.warning(f"Timeout for {url}, attempt {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logging.error(f"Error fetching {url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None

    def parse_source(self, html_content, source_config):
        """Парсит HTML-контент одного источника."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            items = soup.select(source_config['item_selector'])
            parsed_items = []

            for item in items[:5]: # Берем первые 5 новостей с каждого сайта
                try:
                    # Получаем заголовок
                    title_elem = None
                    for selector in source_config['title_selector'].split(', '):
                        title_elem = item.select_one(selector.strip())
                        if title_elem:
                            break
                    if not title_elem:
                        continue
                    title = title_elem.get_text(strip=True)
                    if not title:
                        continue

                    # Получаем ссылку
                    link_elem = None
                    for selector in source_config['link_selector'].split(', '):
                        link_elem = item.select_one(selector.strip())
                        if link_elem:
                            break
                    if not link_elem:
                        continue
                    link = link_elem.get('href')
                    if not link:
                        continue
                    if link.startswith('/'):
                        link = urljoin(source_config['base_url'], link)
                    elif not link.startswith('http'):
                        link = urljoin(source_config['base_url'], link)

                    # Получаем описание
                    description = ""
                    if 'description_selector' in source_config:
                        desc_elem = None
                        for selector in source_config['description_selector'].split(', '):
                            desc_elem = item.select_one(selector.strip())
                            if desc_elem:
                                break
                        if desc_elem:
                            description = desc_elem.get_text(strip=True)[:200]

                    # Получаем изображение
                    image_url = None
                    if 'image_selector' in source_config:
                        img_elem = None
                        for selector in source_config['image_selector'].split(', '):
                            img_elem = item.select_one(selector.strip())
                            if img_elem:
                                break
                        if img_elem:
                            image_url = img_elem.get('src') or img_elem.get('data-src')
                            if image_url:
                                if image_url.startswith('/'):
                                    image_url = urljoin(source_config['base_url'], image_url)
                                elif not image_url.startswith('http'):
                                    image_url = urljoin(source_config['base_url'], image_url)

                    news_item = {
                        'source': source_config['name'],
                        'title': title.strip(),
                        'url': link,
                        'description': description.strip() if description else "",
                        'image': image_url
                    }
                    parsed_items.append(news_item)
                except Exception as e:
                    logging.warning(f"Ошибка обработки элемента {source_config['name']}: {e}")
                    continue
            return parsed_items
        except Exception as e:
            logging.error(f"Ошибка парсинга {source_config['name']}: {e}")
            return []

    def format_news_post(self, item):
        """Форматирует новостной пост."""
        post = f"📰 <b>{item['title']}</b>\n\n"
        if item['description']:
            post += f"{item['description']}\n\n"
        post += f"📍 {item['source']}\n"
        post += f"🔗 <a href='{item['url']}'>Read more</a>"
        return post

    def format_release_post(self, item):
        """Форматирует пост о релизе."""
        post = f"🔥 <b>{item['title']}</b>\n\n"
        if item['description']:
            post += f"{item['description']}\n\n"
        post += f"📍 {item['source']}\n"
        post += f"🔗 <a href='{item['url']}'>Chart on Billboard</a>"
        return post

    async def post_single_item(self, session, item, channel, formatter, item_type):
        """Публикует один элемент."""
        if self.is_posted(item['url'], item_type):
            return False
        try:
            post_text = formatter(item)
            if item.get('image'):
                try:
                    await self.bot.send_photo(
                        chat_id=channel,
                        photo=item['image'],
                        caption=post_text,
                        parse_mode='HTML'
                    )
                    print(f"📸 {item['source']}: {item['title'][:40]}...")
                except Exception as e:
                    logging.warning(f"Не удалось отправить фото: {e}")
                    await self.bot.send_message(
                        chat_id=channel,
                        text=post_text,
                        parse_mode='HTML',
                        disable_web_page_preview=False
                    )
                    print(f"📄 (без фото) {item['source']}: {item['title'][:40]}...")
            else:
                await self.bot.send_message(
                    chat_id=channel,
                    text=post_text,
                    parse_mode='HTML',
                    disable_web_page_preview=False
                )
                print(f"📄 {item['source']}: {item['title'][:40]}...")
            
            self.mark_as_posted(item['url'], item_type)
            return True
        except Exception as e:
            logging.error(f"Ошибка публикации: {e}")
            return False

    async def post_news_cycle(self, session):
        """Цикл публикации 3 новостей каждые 2 часа."""
        print(f"\n[{datetime.now(WASHINGTON_TZ).strftime('%H:%M:%S %Z')}] 🚀 Сбор новостей...")
        all_news = []
        
        # Собираем новости со всех сайтов
        for source in NEWS_SOURCES:
            print(f"📡 {source['name']}...", end=" ")
            html = await self.fetch_url(session, source['url'])
            if html:
                news = self.parse_source(html, source)
                all_news.extend(news)
                print(f"✅ {len(news)}")
            else:
                print("❌")

        if not all_news:
            print("📭 Новостей не найдено")
            return 0

        # Постим новые новости (максимум 3)
        posted_count = 0
        for item in all_news[:3]:
            if await self.post_single_item(session, item, NEWS_CHANNEL, self.format_news_post, 'news'):
                posted_count += 1
                await asyncio.sleep(2) # Пауза между постами
        
        print(f"[{datetime.now(WASHINGTON_TZ).strftime('%H:%M:%S %Z')}] 📈 Опубликовано: {posted_count} новостей")
        return posted_count

    async def post_releases_daily(self, session):
        """Ежедневная подборка релизов в 10:00 ET."""
        now_et = datetime.now(WASHINGTON_TZ)
        print(f"\n[{now_et.strftime('%H:%M:%S %Z')}] 🚀 Сбор релизов для подборки...")
        
        all_releases = []
        # Собираем релизы со всех сайтов
        for source in RELEASE_SOURCES:
            print(f"📡 {source['name']}...", end=" ")
            html = await self.fetch_url(session, source['url'])
            if html:
                releases = self.parse_source(html, source)
                all_releases.extend(releases)
                print(f"✅ {len(releases)}")
            else:
                print("❌")

        if not all_releases:
            print("📭 Релизов не найдено")
            return 0

        # Постим новые релизы (максимум 5)
        posted_count = 0
        for item in all_releases[:5]:
            if await self.post_single_item(session, item, RELEASES_CHANNEL, self.format_release_post, 'release'):
                posted_count += 1
                await asyncio.sleep(1) # Меньше пауза для релизов
        
        print(f"[{datetime.now(WASHINGTON_TZ).strftime('%H:%M:%S %Z')}] 📈 Опубликовано: {posted_count} релизов")
        return posted_count

# --- Планировщик ---

async def run_scheduler(bot, session):
    """Планировщик задач."""
    # Запуск новостей сразу при старте
    await bot.post_news_cycle(session)
    
    # Планирование
    schedule.every(2).hours.do(lambda: asyncio.create_task(bot.post_news_cycle(session)))
    schedule.every().day.at("10:00").do(lambda: asyncio.create_task(bot.post_releases_daily(session)))
    
    print(f"⏳ Планировщик запущен.")
    print(f"🕒 Новости: каждые 2 часа")
    print(f"🕒 Релизы: каждый день в 10:00 ET (Вашингтон)")
    
    while True:
        schedule.run_pending()
        await asyncio.sleep(60) # Проверка каждую минуту

# --- Запуск ---

async def main():
    """Основная асинхронная функция."""
    print("🔄 ЗАПУСК БОТА HIP-HOP (ET Schedule)")
    print(f"Канал для новостей: {NEWS_CHANNEL}")
    print(f"Канал для релизов: {RELEASES_CHANNEL}")
    print("Для остановки нажмите Ctrl+C")
    
    bot = HipHopETBot(TOKEN)
    
    # Создаем aiohttp сессию с ограничением на количество соединений
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=3, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            await run_scheduler(bot, session)
        except KeyboardInterrupt:
            print("\n👋 Бот остановлен.")

if __name__ == "__main__":
    asyncio.run(main())
