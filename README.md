## Obtaining Data
I chose to obtain the data by calling an API. I wrote a basic Python script, `repo_data.py`, which allows the user to extract, transform, and load data from any public GitHub repository (so that it can be used for further analysis). The `requests` library is used to make HTTP requests to the [GitHub REST API](https://docs.github.com/en/rest) and save the data locally in `json` format. This structured data is then parsed and stored in a local SQL database using `sqlite3`.

The script serves as a flexible extract, transform, load (ETL) tool that integrates with the GitHub REST API. For example, regardless of the size or the activity level of a repository, it can be supplied (in global variable `OWNER_REPO`) and the script will execute all the same.

Currently, the script only retrieves data concerning contributors, commits, issues, and pulls; however, API endpoints can easily be added or removed as needed (though the API rate limit is, obviously, a limiting factor) by referencing the [documentation](https://docs.github.com/en/rest/reference) for the GitHub REST API.

### GitHub API Rate Limit
The GitHub API Rate Limit was an initial obstacle:
> The GitHub API rate limit ensures that the API is fast and available for everyone.
> 
> If you hit a rate limit, it's expected that you back off from making requests and try again later when you're permitted to do so. Failure to do so may result in the banning of your app.
> 
> You can always check your rate limit status at any time. Checking your rate limit incurs no cost against your rate limit.
> 
> &mdash; [GitHub](https://docs.github.com/en/rest/guides/best-practices-for-integrators#dealing-with-rate-limits)

When requests are authorized with a [generated personal access token](https://github.com/settings/tokens), the user is limited to 5,000 requests per hour. Fortunately, the response is formatted in JSON and paginated, and with the `per_page` URL parameter, the number of items per page can be increased from 30 to 100 (the maximum). This is a particularly important optimization that allowed me to retrieve tens of thousands of commits, issues, and pull requests, since an API request is made for each page.

## Sample Questions
Once the data was modeled in a SQL database, I could answer the following questions by executing simple SQL queries:

**What is the average number of pull requests per contributor?**
```
WITH cte AS 
(
	SELECT COUNT(1) AS num_pulls  
	FROM pulls 
	GROUP BY created_by
)
SELECT AVG(num_pulls) 
FROM cte
```
The average number of pull requests per contributor is ~4.6.

**What is the maximum number of commits per week?**
```
SELECT COUNT(1) AS num_commits, STRFTIME('%W %Y', date_committed) as week 
FROM commits 
GROUP BY week
ORDER BY num_commits DESC
```
The maximum number of commits per week is 1,772, which occurred in week 12 of 2019.

**What is the minimum number of days of an issue being open?**
```
SELECT MIN(JULIANDAY('now') - JULIANDAY(date_created)) AS min_days 
FROM issues 
WHERE state='open'
```
The minimum number of days of an issue being open (that is currently open) is ~2 days.
```
SELECT MIN(JULIANDAY(date_closed) - JULIANDAY(date_created)) AS min_days 
FROM issues 
WHERE state='closed'
```
The minimum number of days of an issue being open (which has been closed) is ~4.6 days.

## Activity Report
After creating some visualizations using Tableau, I wrote a short report of the activity in the public GitHub repository [tensorflow/tensorflow](https://github.com/tensorflow/tensorflow), an end-to-end open-source platform for machine learning. In my report, `activity_report.pdf`, I summarize activity statistics and trends, and I include a few graphics.

---

## Possible Improvements
If I were to spend more time improving this project, there are a few different things I would do:
- I would look into decreasing the runtime of `repo_data.py`. Currently, I do not think it will exceed the API rate limit of 5,000 requests per hour because, ironically, it takes longer than an hour to perform 5,000 requests (it is worth mentioning that I only needed about 2,500 requests to retrieve all of the data from tensorflow/tensorflow).
- I would add more API endpoints, so that I can collect more data apart from contributors, commits, issues, and pull requests. Furthermore, I would adjust the script so that it delves into "deeper" API endpoints by using the URLS contained within them.
- I would spend more time creating visualizations in Tableau and coming up with different ways to give insight into the activity in the repository. Perhaps I would collect data on other popular repositories and compare them to each other.
