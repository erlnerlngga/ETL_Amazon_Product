from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


from time import sleep
from datetime import datetime
import random
import json

def start_driver():
    # path = "C:\Program Files (x86)\chromedriver.exe"
    # options = webdriver.ChromeOptions()
    # options.add_argument('--ignore-certificate-errors')
    # options.add_argument('--ignore-ssl-errors')
    driver = webdriver.Remote('http://selenium:4444/wd/hub', desired_capabilities=DesiredCapabilities.CHROME)
    return driver

def generate_url(item, category, page):
    scc = f"https://www.amazon.com/s?k={item}&i={category}-intl-ship&page={page}&crid=259E6E2J69G4W&sprefix=lapt%2Ccomputers-intl-ship%2C705&ref=nb_sb_noss_2" 
    return scc
    
def long_page(driver):
    len_page = driver.find_element_by_xpath('.//span[@class = "s-pagination-strip"]')
    if len(len_page.text) == 20:
        return int(len_page.text[-6:-4]) + 1
    else:
        return int(len_page.text[-7:-4]) + 1
    
def sleep_little_bit():
    time_in_seconds = random.randint(1, 3) * 10
    sleep(time_in_seconds) 
    
def save_data(item, category, data):
    title = datetime.now().strftime("%m%Y")
    name = f'{item}-{category}-{title}.json'
    
    with open(name, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def extract_data(i):
    
    description = i.find_element_by_xpath('//h2/a').text.strip()
    url = i.find_element_by_xpath('//h2/a').get_attribute('href')
    
    try:
        price = i.find_element_by_xpath('.//span[@class="a-price-whole"]').text
    except NoSuchElementException:
        price=0
    
    try:
        temp = i.find_element_by_xpath('.//span[contains(@aria-label, "out of")]')
        rating = temp.get_attribute('aria-label')
    except NoSuchElementException:
        rating=""   
    
    try:
        temp = i.find_element_by_xpath('.//span[contains(@aria-label, "out of")]/following-sibling::span')
        review_count = temp.get_attribute('aria-label')
    except NoSuchElementException:
        review_count=""
    
    result = {'Description':description, 'Price':price, 'Ratings':rating, 'Review_Count':review_count, 'Url':url}
    return result

    
def get_product_page(driver):
    cards = driver.find_elements_by_xpath('//div[@data-component-type="s-search-result"]')
    return cards

def run(item, category):
    sleep_again = 2000
    result = []
    num_records_scraped = 0
    
    driver = start_driver()
    base = f"https://www.amazon.com/s?k={item}&i={category}-intl-ship&crid=259E6E2J69G4W&sprefix=lapt%2Ccomputers-intl-ship%2C705&ref=nb_sb_noss_2"
    driver.get(base)
    page_long = long_page(driver)
    
    for page in range(1, page_long):
        if page > 1:
            url = generate_url(item, category, page)
            driver.get(url)
        
        product_page = get_product_page(driver)
        for product in product_page:
            record = extract_data(product)
            if record:
                result.append(record)
                num_records_scraped += 1
        
        if num_records_scraped == sleep_again:
            sleep(300)
            sleep += 2000
        sleep_little_bit()
        
    save_data(item, category, result)
    # print(f"Total record: {num_records_scraped}")
    driver.quit()

# if __name__ == '__main__':   
#     dta = [('laptop', 'computers'), ('phones', 'electronics'), ('bedroom', 'kitchen'), ('furniture', 'kitchen'), ('tablet', 'computers'), ('camera', 'electronics'), ('cookware','kitchen')]
#     for pro, cat in dta:
#         run(pro, cat)

