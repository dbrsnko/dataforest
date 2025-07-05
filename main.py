import requests
from lxml import html
import psycopg2
from psycopg2 import sql
from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict
import threading
import asyncio
import queue
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv

load_dotenv()

class Category(Enum):
    """Enum for product categories we're scraping."""
    DEVOPS = "DevOps"
    IT_INFRASTRUCTURE = "IT Infrastructure"
    DATA_ANALYTICS = "Data Analytics and Management"

@dataclass
class Product:
    """Data class representing a product from Vendr.com. or potentially any other website"""
    name: str
    category: Category
    median_price: str
    lower_range: str
    upper_range: str
    description: str


class DatabaseManager(ABC):
    @abstractmethod
    def _create_connection(self):
        pass

    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def save_product(self, product: Product):
        pass

class PostgresManager(DatabaseManager):
    """Handles all database operations using psycopg2."""
    
    def __init__(self):
        self._ensure_database_exists()
        self.connection = self._create_connection()
        self._initialize_tables()
        
    def _create_connection(self):
        """Create a PostgreSQL database connection."""
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
    
    def _ensure_database_exists(self):
        """Check if database exists and create it if not."""
        try:
            # Connect to PostgreSQL default database to check if our DB exists            
            conn = psycopg2.connect(
                dbname="postgres",
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            conn.autocommit=True
            cursor = conn.cursor()
            print("Connection established")
            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (os.getenv("DB_NAME"),))
            exists = cursor.fetchone()
            
            if not exists:
                print(f"Database {os.getenv('DB_NAME')} does not exist. Creating it...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(os.getenv("DB_NAME")))
                )
                print(f"Database {os.getenv('DB_NAME')} created successfully.")
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error ensuring database exists: {e}")
            raise

    def _initialize_tables(self):
        """Initialize the database with required tables."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    category VARCHAR(50) NOT NULL,
                    median_price VARCHAR(100),
                    lower_range VARCHAR(100),       
                    upper_range VARCHAR(100),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.connection.commit()
    
    def save_product(self, product: Product):
        """Save a product to the database."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO products (name, category, median_price, lower_range, upper_range, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (product.name, product.category.value, product.median_price, product.lower_range, product.upper_range,  product.description))
            self.connection.commit()
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()

class Scraper(ABC):
    @abstractmethod
    def run(self, num_workers):
        pass

class VendrScraper(Scraper):
    """Main scraper class for Vendr.com products."""
    
    BASE_URL = "https://www.vendr.com"
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.task_queue = queue.Queue()
        self.scraped_products = queue.Queue()
        self.stop_event = threading.Event()
        
    def get_category_urls(self) -> Dict[Category, str]:
        """Get URLs for each category we want to scrape."""
        categories = {
            Category.DEVOPS: "devops",
            Category.IT_INFRASTRUCTURE: "it-infrastructure",
            Category.DATA_ANALYTICS: "data-analytics-and-management",
        }
        return {category: f"{self.BASE_URL}/categories/{path}" 
            for category, path in categories.items()}
        
    
    def fetch_page(self, url: str) -> Optional[html.HtmlElement]:
        """Fetch and parse a webpage."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
        }
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=10,
                allow_redirects=True
            )
            response.raise_for_status()
            return html.fromstring(response.content)
        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def extract_product_links(self, category_page: html.HtmlElement) -> List[str]:
        """Extract product links from a category page."""
        product_links = category_page.xpath('//a[contains(@class, "rt-Link") and contains(@href, "/marketplace/")]/@href')
        return [f"{self.BASE_URL}{link}" for link in product_links if link]        
    
    def extract_product_details(self, product_page: html.HtmlElement, category: Category) -> Optional[Product]:
        """Extract product details from a product page."""
        try:
            name = product_page.xpath('//h1[contains(@class, "rt-Heading")]/text()')[0].strip()

            median_price_text = product_page.xpath('//span[contains(@class, "rt-Text") and contains(text(), "Median:")]/following-sibling::text()[1]')
            median_price = median_price_text[0].strip() if median_price_text else "Price not available"

            lower_range_text = product_page.xpath('//div[contains(@class, "_rangeSlider")]//span[1]/text()')
            lower_range = lower_range_text[0].strip() if lower_range_text else None

            upper_range_text = product_page.xpath('//span[contains(@class, "_rangeSliderLastNumber")]/text()')
            upper_range = upper_range_text[0].strip() if upper_range_text else None

            description_text = product_page.xpath('//div[contains(@class, "rt-Box")]//p[contains(@class, "rt-Text")]/text()')
            description = description_text[0].strip() if description_text else None

            return Product(
                name=name,
                category=category,
                median_price=median_price,
                lower_range=lower_range,
                upper_range=upper_range,
                description=description
            )
        except (IndexError, AttributeError) as e:
            print(f"Error extracting product details: {e}")
            return None
    
    def worker(self):
        """Worker thread that processes tasks from the queue."""
        while not self.stop_event.is_set():
            try:
                task = self.task_queue.get(timeout=1)
                category, product_url = task
                
                product_page = self.fetch_page(product_url)
                if product_page is not None:
                    product = self.extract_product_details(product_page, category)
                    if product:
                        self.scraped_products.put(product)
                
                self.task_queue.task_done()
            except queue.Empty:
                continue
    
    def db_writer(self):
        """Dedicated thread for writing products to the database."""
        while not self.stop_event.is_set() or not self.scraped_products.empty():
            try:
                product = self.scraped_products.get(timeout=1)
                self.db_manager.save_product(product)
                self.scraped_products.task_done()
            except queue.Empty:
                continue
    
    def run(self, num_workers: int = 4):
        """Run the scraper with multiple worker threads."""
        # Get category URLs and populate task queue
        category_urls = self.get_category_urls()
        
        for category, url in category_urls.items():
            category_page = self.fetch_page(url)
            if category_page is not None:
                product_links = self.extract_product_links(category_page)
                for link in product_links:
                    self.task_queue.put((category, link))
        
        # Start worker threads
        workers = []
        for _ in range(num_workers):
            thread = threading.Thread(target=self.worker)
            thread.start()
            workers.append(thread)
        
        # Start database writer thread
        db_thread = threading.Thread(target=self.db_writer)
        db_thread.start()
        
        # Wait for all tasks to be processed
        self.task_queue.join()
        self.scraped_products.join()
        
        # Signal threads to stop
        self.stop_event.set()
        
        # Wait for all threads to finish
        for worker in workers:
            worker.join()
        db_thread.join()


async def main():
    db_manager = PostgresManager()
    scraper = VendrScraper(db_manager)    
    try:
        print("Starting scraping process...")
        scraper.run(num_workers=int(os.getenv("NUM_WORKERS", 4)))
        print("Scraping completed")
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        db_manager.close()


if __name__ == "__main__":
    asyncio.run(main())