import time
from selenium import webdriver
import csv

from selenium.webdriver.support.select import Select
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from bs4 import Tag
from selenium.webdriver.common.by import By
import itertools as IT
from selenium.webdriver.chrome.options import Options
from multiprocessing import Pool
from selenium.common.exceptions import TimeoutException
from datetime import date
import os
import random

jobs = [
    'software+engineer',
    'software+tester',
    'quality+assurance',
    'quality+engineer',
    'system+administrator',
    'system+analyst',
    'system+engineer',
    'solution+architect',
    'web+developer',
    'web+designer',
    'front+end+developer',
    'full+stack+developer',
    'mobile+developer',
    'technical+support',
    'technical+writer',
    'technical+analyst',
    'technical+manager',
    'security+analyst',
    'network+analyst',
    'network+administrator',
    'network+engineer',
    'network+security',
    'network+support',
    'database+analyst',
    'database+developer',
    'database+administrator',
    'data+engineer',
    'data+analyst',
    'data+scientist',
    'data+architect',
    'application+engineer',
    'devops+engineer',
    'program+manager',
    'program+director',
    'development+manager',
    'development+director',
    'game+designer',
    'game+developer',
    'game+tester',
    'cloud+engineer',
    'cloud+architect',
    'cloud+security',
    'cloud+system',
]

us_states = [
    'Alabama',
    'Alaska',
    'Arizona',
    'Arkansas',
    'California',
    'Colorado',
    'Connecticut',
    'Delaware',
    'District+of+Columbia',
    'Florida',
    'Georgia',
    'Hawaii',
    'Idaho',
    'Illinois',
    'Indiana',
    'Iowa',
    'Kansas',
    'Kentucky',
    'Louisiana',
    'Maine',
    'Montana',
    'Nebraska',
    'Nevada',
    'New+Hampshire',
    'New+Jersey',
    'New+Mexico',
    'New+York',
    'North+Carolina',
    'North+Dakota',
    'Ohio',
    'Oklahoma',
    'Oregon',
    'Maryland',
    'Massachusetts',
    'Michigan',
    'Minnesota',
    'Mississippi',
    'Missouri',
    'Pennsylvania',
    'Rhode+Island',
    'South+Carolina',
    'South+Dakota',
    'Tennessee',
    'Texas',
    'Utah',
    'Vermont',
    'Virginia',
    'Washington',
    'West+Virginia',
    'Wisconsin',
    'Wyoming',
]

look_back_range = 1
# print(start_list)
ca_base_url = 'https://ca.indeed.com'
us_base_url = "https://www.indeed.com"

today = date.today()

ca_file_name = f'e:/BCIT/COMP8047/daily-job/result/ca_job_results_{today}.csv'
us_file_name = f'e:/BCIT/COMP8047/daily-job/result/us_job_results_{today}.csv'

options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--allow-running-insecure-content')
options.add_argument('--ignore-certificate-errors')
options.add_argument('-allow-insecure-localhost')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)
driver.implicitly_wait(20)
driver.set_page_load_timeout(20)
# driver.minimize_window()
driver.get("https://www.google.com/")
# driver.set_window_position(-2000, 0)

def load_results(params):
    # print(params)
    loc, job_cat, fromage, country = params
    
    base_url = us_base_url if country == 'us' else ca_base_url
    file_name = us_file_name if country == 'us' else ca_file_name

    # for start in loop_page_list:
    #     url = base_url + f'/jobs?q={job_cat}&l={loc}&fromage={fromage}&start={start}'
    #     print(url)
    #     try:
    #         driver.execute_script(f"window.open('{url}', 'tab{start}');")
    #         time.sleep(1)
    #     except TimeoutException:
    #         pass

    with open(file_name, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # writer.writerow(['Job Title', 'Company', 'Location', 'Job Link', 'Salary'])
        # writer.writerow(['Job ID', 'Job Title', 'Company', 'Location', 'Salary', 'post_day', 'load_date'])

        # driver.switch_to.window(f'tab{start}')
        url = base_url + f'/jobs?q={job_cat}&l={loc}&fromage={fromage}'
        print(url)
        try:
            driver.get(url)
            time.sleep(random.uniform(3,5))
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            if soup.find('ul', {'id': 'filter-jobtype-menu'}):
                drop = soup.find('ul', {'id': 'filter-jobtype-menu'})
                # print(drop)
                for li in drop.find_all("li"):
                    try:
                        # print(li.text)
                        job_type = li.text.split()[0]
                        count_element = li.text.split()[1].strip().replace('(', '').replace(')', '')
                        # print(job_cat, loc, job_type, count_element)
                        writer.writerow([job_cat, loc, job_type, count_element, fromage, today])
                    except Exception as e: 
                        print(e)
                        pass

            items = soup.find('td', {'class': 'resultContent'})
            if soup.find_all('div', {'class': 'jobsearch-JobCountAndSortPane-jobCount css-13jafh6 eu4oa1w0'}):
                all_count_element = \
                soup.find_all('div', {'class': 'jobsearch-JobCountAndSortPane-jobCount css-13jafh6 eu4oa1w0'})[0].find(
                    "span").text.split()[0]
                # print(date_element[0].find("span").text)
                writer.writerow([job_cat, loc, 'all', all_count_element, fromage, today])
        except Exception as e: 
            print(e)
            pass


if __name__ == '__main__':
    with open(us_file_name, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if os.stat(us_file_name).st_size == 0:
            writer.writerow(
                ['job_cat', 'loc', 'job_type', 'job_count', 'prev_range', 'search_date'])
    us_params = []

    for location in us_states:
        for job in jobs:
            us_params.append([location, job, look_back_range, 'us'])
    
    pool = Pool(processes=5)
    pool.map(load_results, us_params)
    pool.close()
    pool.join()

    driver.close()
    driver.quit()