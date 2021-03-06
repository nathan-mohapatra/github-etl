"""
This script allows the user to extract, transform, and load data from any public GitHub repository (so that it can be
used for further analysis). The requests library is used to make HTTP requests to the GitHub REST API and save the data
locally in JSON format. This structured data is then parsed and stored in a SQL database using SQLite.

This script serves as a flexible ETL tool that integrates with the GitHub REST API. For example, regardless of the size
or the activity level of a repository, it can be supplied (in `OWNER_REPO`) and this script will execute all the same.

Currently, this script only retrieves data concerning commits, contributors, issues, and pulls; however, API endpoints
can easily be added or removed as needed (though the API rate limit is, obviously, a limiting factor) by referencing
https://docs.github.com/en/rest/reference.

This script requires that `requests` be installed within the Python environment you are running it in.

This file can be imported as a module and contains the following functions:
    * get_rate_limit: Tracks current usage of the GitHub REST API.
    * extract: Given a public GitHub repository and a GitHub REST API endpoint, retrieves associated data in JSON
    format.
    * transform_load: Given the data retrieved from a GitHub REST API endpoint in JSON format, creates and populates
    a table in a SQL database.
    * main: Establishes connection and interactivity with SQL database; performs ETL operations on predetermined GitHub
    REST API endpoints.
"""

import json
import requests
import sqlite3
import sys

# Go to https://github.com/settings/tokens and generate a new personal access token
# Once authorized with this token, the user is limited to 5000 requests to the GitHub REST API per hour
ACCESS_TOKEN = 'Your personal access token'
RATE_LIMIT = 5000

# The name of any public GitHub repository
OWNER_REPO = 'tensorflow/tensorflow'


def get_rate_limit(headers):
    """
    Tracks current usage of the GitHub REST API.

    :param headers: The HTTP authorization request header.
    :return: The number of remaining API requests available.
    """
    url = 'https://api.github.com/rate_limit'
    res = requests.get(url, headers=headers)
    remaining = int(res.json()['resources']['core']['remaining'])

    print(f'API request usage: {RATE_LIMIT - remaining}/{RATE_LIMIT}')

    return remaining


def extract(endpoint, has_state=False):
    """
    Given a public GitHub repository and a GitHub REST API endpoint, retrieves associated data in JSON format.

    :param endpoint: The final "point of entry" in the GitHub REST API.
    :param has_state: Whether JSON objects have a `state` key.
    :return: The response to the request in JSON format.
    """
    # HTTP authorization request header
    headers = {'authorization': 'token ' + ACCESS_TOKEN}

    # Check if API rate limit is exceeded
    if get_rate_limit(headers):
        print(f'Extracting {endpoint} from {OWNER_REPO}...')

        # Items are paginated; `per_page` parameter can increase items per page to 100 (maximum)
        # This is a particularly important optimization, since an API request will be made for each page
        url = f'https://api.github.com/repos/{OWNER_REPO}/{endpoint}?per_page=100'
        if has_state:
            url += '&state=all'  # Include items with state=closed

        # Iterate through pages and save response
        res = requests.get(url, headers=headers)
        json_data = res.json()
        while 'next' in res.links.keys():
            try:
                res = requests.get(res.links['next']['url'], headers=headers)
                json_data.extend(res.json())

            # API rate limit is exceeded
            except:
                print('API rate limit exceeded...')
                return None

        print('Success!')
        get_rate_limit(headers)

        return json_data

    else:
        print('API rate limit exceeded...')
        return None


def transform_load(conn, curs, json_data, endpoint):
    """
    Given the data retrieved from a GitHub REST API endpoint in JSON format, creates and populates a table in a SQL
    database.

    :param conn: The `Connection` object that represents a SQL database.
    :param curs: The `Cursor` object that interacts with a SQL database.
    :param json_data: The structured data that is parsed and stored.
    :param endpoint: The GitHub REST API endpoint which indicates how the data is structured.
    """
    print(f'Transforming and loading {endpoint} in "{OWNER_REPO.split("/")[1]}_repo.db"...')

    # Schema for contributors table
    if endpoint == 'contributors':
        curs.execute("""
            CREATE TABLE IF NOT EXISTS contributors(
                id INTEGER PRIMARY KEY,
                node_id VARCHAR(255) NOT NULL,
                login VARCHAR(255) NOT NULL,
                contributions INTEGER NOT NULL
            );
            """)

    # Schema for commits table
    elif endpoint == 'commits':
        curs.execute("""
            CREATE TABLE IF NOT EXISTS commits(
                sha VARCHAR(255) PRIMARY KEY,
                tree_sha VARCHAR(255) NOT NULL,
                parents_sha TEXT,
                node_id VARCHAR(255) NOT NULL,
                author VARCHAR(255),
                date_authored VARCHAR(255) NOT NULL,
                committer VARCHAR(255),
                date_committed VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                comments INTEGER NOT NULL,
                FOREIGN KEY(author) REFERENCES contributors(login) ON DELETE SET NULL,
                FOREIGN KEY(committer) REFERENCES contributors(login) ON DELETE SET NULL
            );
            """)

    # Schema for issues table
    elif endpoint == 'issues':
        curs.execute("""
            CREATE TABLE IF NOT EXISTS issues(
                id INTEGER PRIMARY KEY,
                node_id VARCHAR(255) NOT NULL,
                number INTEGER NOT NULL,
                state VARCHAR(255) NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                assignees TEXT,
                labels TEXT,
                comments INTEGER NOT NULL,
                created_by VARCHAR(255) NOT NULL,
                date_created VARCHAR(255) NOT NULL,
                date_updated VARCHAR(255) NOT NULL,
                date_closed VARCHAR(255),
                FOREIGN KEY(created_by) REFERENCES contributors(login) ON DELETE SET NULL
            );
            """)

    # Schema for pulls table
    elif endpoint == 'pulls':
        curs.execute("""
            CREATE TABLE IF NOT EXISTS pulls(
                id INTEGER PRIMARY KEY,
                node_id VARCHAR(255) NOT NULL,
                number INTEGER NOT NULL,
                state VARCHAR(255) NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                assignees TEXT,
                reviewers TEXT,
                labels TEXT,
                created_by VARCHAR(255) NOT NULL,
                date_created VARCHAR(255) NOT NULL,
                date_updated VARCHAR(255) NOT NULL,
                date_closed VARCHAR(255),
                date_merged VARCHAR(255),
                merge_sha VARCHAR(255),
                head_sha VARCHAR(255) NOT NULL,
                base_sha VARCHAR(255) NOT NULL,
                FOREIGN KEY(created_by) REFERENCES contributors(login) ON DELETE SET NULL
            );
            """)

    columns = None
    for item in json.loads(json_data):
        # Columns and values for contributors table
        if endpoint == 'contributors':
            data_dict = {
                'id': item['id'],
                'node_id': item['node_id'],
                'login': item['login'],
                'contributions': item['contributions']
            }

        # Columns and values for commits table
        elif endpoint == 'commits':
            data_dict = {
                'sha': item['sha'],
                'tree_sha': item['commit']['tree']['sha'],
                'parents_sha': ','.join([parent['sha'] for parent in item['parents']]),
                'node_id': item['node_id'],
                'author': item['author']['login'] if item['author'] else None,
                'date_authored': item['commit']['author']['date'],
                'committer': item['committer']['login'] if item['committer'] else None,
                'date_committed': item['commit']['committer']['date'],
                'message': item['commit']['message'],
                'comments': item['commit']['comment_count']
            }

        # Columns and values for issues table
        elif endpoint == 'issues':
            data_dict = {
                'id': item['id'],
                'node_id': item['node_id'],
                'number': item['number'],
                'state': item['state'],
                'title': item['title'],
                'body': item['body'],
                'assignees': ','.join([assignee['login'] for assignee in item['assignees']]),
                'labels': ','.join([label['name'] for label in item['labels']]),
                'comments': item['comments'],
                'created_by': item['user']['login'],
                'date_created': item['created_at'],
                'date_updated': item['updated_at'],
                'date_closed': item['closed_at']
            }

        # Columns and values for pulls table
        elif endpoint == 'pulls':
            data_dict = {
                'id': item['id'],
                'node_id': item['node_id'],
                'number': item['number'],
                'state': item['state'],
                'title': item['title'],
                'body': item['body'],
                'assignees': ','.join([assignee['login'] for assignee in item['assignees']]),
                'reviewers': ','.join([reviewer['login'] for reviewer in item['requested_reviewers']]),
                'labels': ','.join([label['name'] for label in item['labels']]),
                'created_by': item['user']['login'],
                'date_created': item['created_at'],
                'date_updated': item['updated_at'],
                'date_closed': item['closed_at'],
                'date_merged': item['merged_at'],
                'merge_sha': item['merge_commit_sha'],
                'head_sha': item['head']['sha'],
                'base_sha': item['base']['sha']
            }

        else:
            print('Invalid endpoint...')
            return

        if not columns:
            columns = list(data_dict.keys())

        # Insert data into associated table
        query = 'INSERT OR IGNORE INTO {table}({columns}) VALUES ({values})'.format(table=endpoint,
                                                                                    columns=','.join(columns),
                                                                                    values=','.join(['?'] * len(columns)))
        values = [data_dict[col] for col in columns]
        curs.execute(query, values)

        # Commit changes to database
        conn.commit()

    print('Success!')


def main():
    """
    Establishes connection and interactivity with SQL database; performs ETL operations on predetermined GitHub REST API
    endpoints.
    """
    # Establish connection and interactivity with SQL database
    conn = sqlite3.connect(f'{OWNER_REPO.split("/")[1]}_repo.db')
    curs = conn.cursor()

    # ETL on contributors
    contributors = json.dumps(extract(endpoint='contributors'))
    transform_load(conn, curs, contributors, endpoint='contributors') if contributors else sys.exit()

    # ETL on commits
    commits = json.dumps(extract(endpoint='commits'))
    transform_load(conn, curs, commits, endpoint='commits') if commits else sys.exit()

    # ETL on issues
    issues = json.dumps(extract(endpoint='issues', has_state=True))
    transform_load(conn, curs, issues, endpoint='issues') if issues else sys.exit()

    # ETL on pulls
    pulls = json.dumps(extract(endpoint='pulls', has_state=True))
    transform_load(conn, curs, pulls, endpoint='pulls') if pulls else sys.exit()

    # Close connection to database
    conn.close()


if __name__ == '__main__':
    main()
