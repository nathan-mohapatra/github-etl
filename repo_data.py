import json
import requests
import sqlite3
import sys

ACCESS_TOKEN = 'Your personal access token'
RATE_LIMIT = 5000

OWNER_REPO = 'tensorflow/tensorflow'


def get_rate_limit(headers):
    url = 'https://api.github.com/rate_limit'
    res = requests.get(url, headers=headers)
    remaining = int(res.json()['resources']['core']['remaining'])

    print(f'API request usage: {RATE_LIMIT - remaining}/{RATE_LIMIT}')

    return remaining


def extract():
    headers = {'authorization': 'token ' + ACCESS_TOKEN}

    if get_rate_limit(headers):
        print(f'Getting contributors from {OWNER_REPO}...')

        url = f'https://api.github.com/repos/{OWNER_REPO}/contributors?per_page=100'
        res = requests.get(url, headers=headers)
        contributors = res.json()
        while 'next' in res.links.keys():
            try:
                res = requests.get(res.links['next']['url'], headers=headers)
                contributors.extend(res.json())

            except:
                print('Rate limit exceeded...')
                return None

        print('Success!')
        get_rate_limit(headers)

        return contributors

    else:
        print('Rate limit exceeded...')
        return None


def transform_load(conn, curs, contributors):
    print(f'Parsing and storing contributors in "{OWNER_REPO.split("/")[1]}_repo.db"...')

    curs.execute("""
        CREATE TABLE IF NOT EXISTS contributors(
        id INTEGER NOT NULL,
        node_id VARCHAR(255) NOT NULL,
        login VARCHAR(255) NOT NULL,
        contributions INTEGER NOT NULL,
        PRIMARY KEY (id, node_id, login)
        );
        """)

    columns = None
    for contributor in json.loads(contributors):
        data_dict = {
            'id': contributor['id'],
            'node_id': contributor['node_id'],
            'login': contributor['login'],
            'contributions': contributor['contributions']
        }

        if not columns:
            columns = list(data_dict.keys())

        query = 'INSERT OR IGNORE INTO contributors({columns}) VALUES ({values})'.format(columns=','.join(columns),
                                                                                         values=','.join(['?'] * len(columns)))
        values = [data_dict[col] for col in columns]
        curs.execute(query, values)

        conn.commit()

    print('Success!')


def main():
    conn = sqlite3.connect(f'{OWNER_REPO.split("/")[1]}_repo.db')
    curs = conn.cursor()

    contributors = json.dumps(extract())
    transform_load(conn, curs, contributors) if contributors else sys.exit()

    conn.close()


if __name__ == '__main__':
    main()
