Web scraper for Vendr.com according to the next task:
  Collect all products from the website https://www.vendr.com/ :  
  Collect products from only three categories: DevOps, IT Infrastructure, Data Analytics and Management
  The following data should be collected: product name, category, price range, and description (refer to the attached photo).  
  The scraping must be implemented with requests for fetching data and lxml for parsing.
  Implement a task queue specifically for managing the product scraping tasks. Each thread should take a task (i.e., a product to scrape) from the queue and process it.  
  Use multithreading in your implementation, with multiple threads fetching product data from the queue and one dedicated thread writing the collected data to the database in parallel.  
  Ensure that all scraping results are stored in the database.
