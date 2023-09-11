"""Scraper for scraping all BU courses and their information"""

import json
import re
from multiprocessing import Pool
from pathlib import Path

import requests
from bs4 import BeautifulSoup


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
        self.class_info: dict = {}


    def run(self):
        """Runs the scraper"""

        self.scrape_branches()
        self.scrape_courses()
        self.save()

    def save(self):
        """Saves the scraped data to a json file"""

        with open(Path(__file__).parent / 'data.json', 'w', encoding='utf-8') as f:
            json.dump(self.class_info, f, indent=4)

    def scrape_branches(self) -> None:
        """Scrapes all branches w/ multiprocessing"""

        with Pool() as p:
            self.class_list = p.map(self.fetch_single_branch, self.branches.keys())

        self.class_list = [item for sublist in self.class_list for item in sublist]

        return

    def fetch_single_branch(self, branch) -> list:
        """Fetches a single branch"""

        url = f'https://www.bu.edu/academics/{branch}/courses/' 
        whole_branch = []

        for i in range(200):
            req = requests.get(url + f'{i}', timeout=(9.05, 27))
            content = BeautifulSoup(req.content, 'html.parser')

            results = content.find('ul', class_='course-feed')
            if len(results.text.strip()) == 0:
                break

            group = []
            for content in results:
                tmp = content.next_sibling
                if tmp is None or len(tmp.text.strip()) == 0:
                    continue
                else:
                    class_code = tmp.text.split(':')[0].strip().replace(' ', '').lower()
                    group.append(class_code)

            for i in group:
                whole_branch.append(i)
        print(f'Fetched: {self.branches[branch]}')
        return whole_branch

    def scrape_courses(self) -> None:
        """Scrapes all courses w/ multiprocessing"""

        with Pool() as p:
            tmp = p.map(self.fetch_single_course, self.class_list)

        self.class_info = {k: v for d in tmp for k, v in d.items()}

        return

    def fetch_single_course(self, course: str) -> dict:
        """Fetches a single course"""

        code = [course[:3], course[3:5], course[5:]]
        url = f'https://www.bu.edu/phpbin/course-search/search.php?page=w0& \
            pagesize=10&adv=1&search_adv_all={code[0]}+{code[1]}+{code[2]}&yearsem_adv=*'

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
        full_list = full.text.splitlines()

        full_dict = {
            'prereq': full_list[1],
            'coreq': full_list[3],
            'description': full_list[5],
            'credit': full_list[6],
            'hub_credit': hub_list[:]
        }
        
        print(f'Fetched: {course}')
        
        return {course: self.cleaner(full_dict)}

    def cleaner(self, contents: dict) -> dict:
        """Cleans the contents of the course"""

        # Description cleaner
        while True:
            if '  ' in contents['description']:
                contents['description'] = contents['description'].replace('  ', ' ')
            else:
                break

        # Credit cleaner
        contents['credit'] = self.filter_numerical(contents['credit'])

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

        return contents

    def filter_numerical(self, string: str) -> str:
        """Filters out all non-numerical characters from a string"""

        result = ''
        for char in string:
            if char in '1234567890':
                result += char
        return result


if __name__ == '__main__':
    scraper = Scraper()
    scraper.run()
