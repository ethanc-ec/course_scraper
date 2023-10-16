"""Scraper for scraping all BU courses and their information"""

import re
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery


class Scraper:
    """Scraper class for scraping all BU courses and their information"""

    def __init__(self):
        self.branches = {
            'khc': 'Kilachand Honors College',
            'busm': 'Chobanian & Avedisian School of Medicine',
            'cas': 'College of Arts and Sciences',
            'com': 'College of Communication',
            'eng': 'College of Engineering',
            'cfa': 'College of Fine Arts',
            'cgs': 'College of General Studies',
            'sar': 'College of Health & Rehabilitation Sciences: Sargent College',
            'cds': 'Faculty of Computing & Data Sciences',
            'gms': 'Graduate Medical Sciences',
            'grs': 'Graduate School of Arts & Sciences',
            'sdm': 'Henry M. Goldman School of Dental Medicine',
            'met': 'Metropolitan College',
            'questrom': 'Questrom School of Business',
            'sha': 'School of Hospitality Administration',
            'law': 'School of Law',
            'sph': 'School of Public Health',
            'ssw': 'School of Social Work',
            'sth': 'School of Theology',
            'wheelock': 'Wheelock College of Education & Human Development',
        }

        self.class_list: list = []
        self.class_info: pd.DataFrame = pd.DataFrame( \
            columns=['course', 'prereq', 'coreq', 'description', 'credit', 'hub_credit'])

        self.parent = Path(__file__).parent

    def run(self) -> None:
        """Runs the scraper"""

        self.scrape_branches()
        self.scrape_courses()
        self.create_csv()
        self.push_to_bigquery()

    def push_to_bigquery(self) -> None:
        """Pushes the scraped data to BigQuery"""

        print('Started: Pushing to BigQuery')
        # Construct a BigQuery client object.
        client = bigquery.Client()

        # Use own table id
        table_id = ""

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        body = pd.read_csv("scraper/courses.csv")
        load_job = client.load_table_from_dataframe(body, table_id, job_config=job_config).result()

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id).num_rows

        if destination_table is not None:
            print(f"Loaded {destination_table} rows.")

        return

    def create_csv(self) -> None:
        """Saves the scraped data to a csv file"""

        self.class_info.to_csv(self.parent / 'courses.csv', index=False)

    def scrape_branches(self) -> None:
        """Scrapes all branches w/ multiprocessing"""

        print('Started: All branches')

        with Pool() as pool:
            self.class_list = pool.map(self.fetch_single_branch, self.branches.keys())

        self.class_list = list(set([item for sublist in self.class_list for item in sublist]))

        print('Completed: All branches')

        return

    def fetch_single_branch(self, branch) -> list:
        """Fetches a single branch"""

        print(f'Started: {self.branches[branch]}')

        url = f'https://www.bu.edu/academics/{branch}/courses/'
        whole_branch = []

        for i in range(200):
            req = requests.get(url + f'{i}', timeout=(9.05, 27))
            content = BeautifulSoup(req.content, 'html.parser')

            results = content.find('ul', class_='course-feed')

            if results is None or len(results.text.strip()) == 0:
                break

            group = []
            for content in results:
                tmp = content.next_sibling if content is not str else content

                if tmp is None or len(tmp.text.strip()) == 0:
                    continue

                else:
                    class_code = tmp.text.split(':')[0].strip().replace(' ', '').lower()
                    group.append(class_code)

            for i in group:
                whole_branch.append(i)

        print(f'Completed: {self.branches[branch]}')

        return whole_branch

    def scrape_courses(self) -> None:
        """Scrapes all courses w/ multiprocessing"""

        print('Started: All courses')

        with Pool() as pool:
            tmp = pool.map(self.fetch_single_course, self.class_list)

        tmp = [i for i in tmp if i is not False]

        self.class_info = pd.DataFrame(tmp)

        print('Completed: All courses')

        return

    def fetch_single_course(self, course: str) -> dict | bool:
        """Fetches a single course"""

        url = f'https://www.bu.edu/phpbin/course-search/search.php?page=w0& \
            pagesize=10&adv=1&search_adv_all={course}&yearsem_adv=*'

        content = BeautifulSoup(requests.get(url, timeout=(9.05, 27)).content, 'html.parser')

        if content.find('div', class_="coursearch-result-content-description") is None:
            cur_datetime = datetime.now()
            semester = 'FALL' if cur_datetime.month > 6 else 'SPRG'
            year = cur_datetime.year

            url = f'https://www.bu.edu/phpbin/course-search/search.php?page=w0&pagesize=10 \
                &adv=1&search_adv_all={course}&yearsem_adv={year}-{semester}'

            content = BeautifulSoup(requests.get(url, timeout=(9.05, 27)).content, 'html.parser')

        hub_list = content.find('ul', class_="coursearch-result-hub-list")
        hub_list = str(hub_list).split('<li>')

        for idx, val in enumerate(hub_list):
            hub_list[idx] = re.sub('<[^>]+>', '', val).strip()

        hub_list = [x for x in hub_list if x != '']

        if hub_list:
            if 'pathway' in hub_list[-1].lower():
                hub_list[-1] = hub_list[-1].split('BU')[0]

        # For finding the prereq, coreq, description and credit
        full = content.find('div', class_="coursearch-result-content-description")

        # Gets: [prereq, coreq,description, numerical credit]
        if full is not None:
            full_list = full.text.splitlines()

        else:
            return False

        full_dict = {
            'course': course,
            'prereq': full_list[1],
            'coreq': full_list[3],
            'description': full_list[5],
            'credit': full_list[6],
            'hub_credit': hub_list[:]
        }

        return cleaner(full_dict)

def cleaner(contents: dict) -> dict:
    """Cleans the contents of the course"""

    # Description cleaner
    while '  ' in contents['description']:
        contents['description'] = contents['description'].replace('  ', ' ')


    # Credit cleaner
    contents['credit'] = filter_numerical(contents['credit'])

    # Removing the 'Prereq:' or 'Coreq:' from the respective entries
    if 'Prereq:' in contents['prereq']:
        contents['prereq'] = contents['prereq'].replace('Prereq:', '')

    if 'Coreq:' in contents['coreq']:
        contents['coreq'] = contents['coreq'].replace('Coreq:', '')

    # Switiching empty strings and arrays to None type, also removes extra whitespace
    for i in contents:
        if any([contents[i] == '', contents[i] == [], \
            contents[i] == ['Part of a Hub sequence']]):
            contents[i] = None

        elif isinstance(contents[i], str):
            contents[i] = contents[i].strip()

            try:
                int(contents[i])

            except ValueError:
                if len(contents[i]) == 1 or len(contents[i]) == 0:
                    contents[i] = None

    return contents

def filter_numerical(string: str) -> str:
    """Filters out all non-numerical characters from a string"""

    result = ''

    if 'var' in string.lower():
        return 'var'

    for char in string:

        try:
            int(char)
            result += char

        except ValueError:
            continue

    return result


if __name__ == '__main__':
    scraper = Scraper()
    scraper.run()
