import requests
import csv
import time
import urllib.parse

SUBREDDIT = "wowthissubexists"
BASE_PHRASE = "Fap Friday"
YEARS = list(range(2013, 2027))
OUTPUT_FILE = "backfill_All_FapFriday_AllYears.csv"
SLEEP_BETWEEN = 2.0
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RedditCrawler/1.0)"
}
COOKIES = {"over18": "1"}

def fetch_posts_for_year(subreddit, phrase, year, writer, seen_ids):
    after = None
    count = 0
    search_phrase = f"{phrase} +{year}"
    query = urllib.parse.quote(search_phrase)
    while True:
        url = f"https://www.reddit.com/r/{subreddit}/search.json?q={query}&type=posts&sort=new&restrict_sr=1&limit=100&include_over_18=on"
        if after:
            url += f"&after={after}"
        resp = requests.get(url, headers=HEADERS, cookies=COOKIES)
        if resp.status_code != 200:
            print(f"Non-200 status {resp.status_code} for {url}")
            break
        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        if not posts:
            print(f"No more posts for year {year}.")
            break
        matched = 0
        for post in posts:
            d = post.get("data", {})
            title = d.get("title", "")
            post_id = d.get("id", "")
            if post_id in seen_ids:
                continue
            if phrase.lower() in title.lower() and str(year) in title:
                row = {
                    "reddit_post_id": post_id,
                    "title": title,
                    "url": f"https://reddit.com{d.get('permalink', '')}",
                    "created_utc": d.get("created_utc", 0),
                    "author": d.get("author", ""),
                }
                writer.writerow(row)
                print(f"WROTE: id={row['reddit_post_id']} author={row['author']} title={row['title'][:80]!r} url={row['url']}")
                matched += 1
                count += 1
                seen_ids.add(post_id)
        after = data.get("data", {}).get("after")
        print(f"Year {year}: Fetched {len(posts)} posts, matched {matched}, after={after}")
        if not after:
            break
        time.sleep(SLEEP_BETWEEN)
    print(f"Year {year}: {count} results written.")

def fetch_all_years(subreddit, base_phrase, years, output_file):
    fieldnames = ["reddit_post_id", "title", "url", "created_utc", "author"]
    seen_ids = set()
    with open(output_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=fieldnames,
            delimiter=",",
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        for year in years:
            fetch_posts_for_year(subreddit, base_phrase, year, writer, seen_ids)
    print(f"Done. All years written to {output_file}")

if __name__ == "__main__":
    fetch_all_years(SUBREDDIT, BASE_PHRASE, YEARS, OUTPUT_FILE)


if __name__ == "__main__":
    fetch_posts(SUBREDDIT, PHRASE, OUTPUT_FILE)
