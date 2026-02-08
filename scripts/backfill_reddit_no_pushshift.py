#!/usr/bin/env python3
"""Backfill Reddit posts without Pushshift.

Usage: set env vars REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
then run: python scripts/backfill_reddit_no_pushshift.py --mode author --value AutoModerator

This script pages by time-windows using Reddit cloudsearch timestamp ranges.
It is best-effort — Reddit search has limits and results may be incomplete for very old data.
"""
import os
import time
import csv
import argparse
from datetime import datetime, timedelta
from typing import Set
from types import SimpleNamespace

import requests
from bs4 import BeautifulSoup
try:
    import praw
except Exception:
    praw = None
from tqdm import tqdm


def epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def make_reddit():
    """Return a PRAW Reddit instance if credentials are present, otherwise None for anonymous mode."""
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "backfill-script/0.1")
    if client_id and client_secret and praw:
        return praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    return None


def search_windows(reddit, subreddit_name: str, query_base: str, start_ts: int, end_ts: int, window_days: int, sleep_between: float, site_wide: bool = False, debug: bool = False):
    """Yield submission-like objects from sequential time windows (start_ts .. end_ts).
    If `reddit` is a PRAW instance, use subreddit.search; otherwise use anonymous JSON scraping from old.reddit.com.
    """
    window_seconds = window_days * 24 * 3600
    cur_end = end_ts
    seen_ids: Set[str] = set()
    headers = {'User-Agent': os.environ.get('REDDIT_USER_AGENT', 'backfill-script/0.1')}

    # No checkpointing — always start from the provided end_ts

    while cur_end > start_ts:
        cur_start = max(start_ts, cur_end - window_seconds)
        cloud_query = f"{query_base} AND timestamp:{cur_start}..{cur_end}"
        # Announce the window being scanned
        try:
            print(f"SCANNING WINDOW: {datetime.fromtimestamp(cur_start).date()} -> {datetime.fromtimestamp(cur_end).date()} (ts {cur_start}-{cur_end})")
        except Exception:
            print(f"SCANNING WINDOW TS: {cur_start}-{cur_end}")

        # For anonymous site-wide searches, avoid timestamp filtering in the query
        # because the site-wide JSON endpoint doesn't respect cloudsearch timestamp ranges reliably.
        if site_wide and not reddit:
            # Paginate site-wide results (no per-window timestamp in query) until we reach start_ts
            headers = {'User-Agent': os.environ.get('REDDIT_USER_AGENT', 'backfill-script/0.1')}
            after = None
            seen_afters = set()
            page_count = 0
            while True:
                params = {
                    'q': query_base,
                    'sort': 'new',
                    'syntax': 'cloudsearch',
                    'limit': '100',
                }
                if after:
                    params['after'] = after
                params['count'] = str(page_count)
                url = "https://old.reddit.com/search.json"
                try:
                    r = requests.get(url, params=params, headers=headers, timeout=20)
                except Exception as e:
                    print(f"HTTP error during site-wide pagination: {e}")
                    break

                if debug:
                    try:
                        print(f"DEBUG: request URL={r.url}")
                    except Exception:
                        pass

                if r.status_code != 200:
                    print(f"Non-200 {r.status_code} for {url} during site-wide pagination")
                    break

                payload = r.json()
                if debug:
                    # dump payload for inspection
                    try:
                        dbg_name = f"debug_sitewide_{cur_start}_{page_count}.json"
                        with open(dbg_name, 'w', encoding='utf-8') as dbgf:
                            import json
                            json.dump(payload, dbgf)
                        print(f"DEBUG: dumped payload to {dbg_name}")
                    except Exception:
                        pass
                data = payload.get('data', {})
                children = data.get('children', [])
                if not children:
                    break

                stop_paging = False
                for item in children:
                    d = item.get('data', {})
                    c = d.get('created_utc')
                    if c is None:
                        continue
                    # If we've gone past the end timestamp, skip
                    if c > cur_end:
                        continue
                    # If we've reached older than start_ts, we can stop entirely
                    if c < start_ts:
                        stop_paging = True
                        break
                    reddit_id = d.get('id')
                    if not reddit_id or reddit_id in seen_ids:
                        continue
                    seen_ids.add(reddit_id)
                    sub = SimpleNamespace(
                        id=reddit_id,
                        created_utc=c,
                        author=d.get('author'),
                        title=d.get('title'),
                        subreddit=SimpleNamespace(display_name=d.get('subreddit')),
                        permalink=d.get('permalink'),
                    )
                    yield sub

                if stop_paging:
                    break

                # compute next after token: prefer data.after, otherwise last child's fullname
                new_after = data.get('after')
                if not new_after:
                    try:
                        last = children[-1].get('data', {})
                        new_after = last.get('name')
                        if new_after:
                            print(f"Pagination: no data.after; falling back to last.name={new_after}")
                    except Exception:
                        new_after = None

                # If no new_after or we've seen it already, stop paging
                if not new_after or new_after in seen_afters or new_after == after:
                    print(f"Pagination stopping: new_after={new_after} seen_before={new_after in seen_afters} same_as_prev={new_after==after}")
                    break

                seen_afters.add(new_after)
                after = new_after
                page_count += len(children)
                time.sleep(sleep_between)

            # After paginating site-wide, we're done
            return

        if reddit:
            subreddit = reddit.subreddit(subreddit_name)
            try:
                # For site-wide searches use the global subreddit 'all' or reddit.search
                if site_wide:
                    results = reddit.subreddit('all').search(cloud_query, sort='new', syntax='cloudsearch', limit=None)
                else:
                    results = subreddit.search(cloud_query, sort='new', syntax='cloudsearch', limit=None)
            except Exception as e:
                print(f"Search error for window {cur_start}-{cur_end}: {e}")
                time.sleep(max(5, sleep_between))
                cur_end = cur_start - 1
                continue

            any_found = False
            found_count = 0
            for sub in results:
                any_found = True
                if sub.id in seen_ids:
                    continue
                seen_ids.add(sub.id)
                found_count += 1
                try:
                    print(f"FOUND (api): id={sub.id} title={getattr(sub,'title','')[:120]!r}")
                except Exception:
                    pass
                yield sub
                # Advance page_count by number of items returned so the next request progresses
                try:
                    page_count += len(children)
                except Exception:
                    page_count += 0
            print(f"API: window returned {found_count} new items")
            time.sleep(sleep_between)
            # advance to next older window
            cur_end = cur_start - 1
            continue

        # Anonymous JSON scraping fallback using old.reddit.com JSON endpoint.
        # We'll paginate results (either site-wide or subreddit-limited) and filter by timestamp range.
        # Build params per-page so tokens/counts are fresh and we can detect no-progress.
        if not site_wide:
            search_base = f"https://old.reddit.com/r/{subreddit_name}/search.json"
        else:
            search_base = "https://old.reddit.com/search.json"

        after = None
        any_found = False
        seen_afters = set()
        page_count = 0
        window_advanced = False
        while True:
            params = {
                'q': query_base,
                'sort': 'new',
                'syntax': 'cloudsearch',
                'limit': '100',
            }
            if not site_wide:
                params['restrict_sr'] = '1'
            if after:
                params['after'] = after
            params['count'] = str(page_count)

            url = search_base
            try:
                r = requests.get(url, params=params, headers=headers, timeout=20)
            except Exception as e:
                print(f"HTTP error for window {cur_start}-{cur_end}: {e}")
                time.sleep(max(5, sleep_between))
                break

            if debug:
                try:
                    print(f"DEBUG: request URL={r.url}")
                except Exception:
                    pass

            if r.status_code == 429:
                ra = None
                try:
                    ra = int(r.headers.get('Retry-After'))
                except Exception:
                    ra = None
                if ra is None:
                    ra = max(30, int(sleep_between * 5))
                print(f"Rate limited (429) for {url}; sleeping {ra}s")
                time.sleep(ra)
                continue
            if r.status_code != 200:
                print(f"Non-200 {r.status_code} for {url} (window {cur_start}-{cur_end})")
                time.sleep(max(5, sleep_between))
                break

            payload = r.json()
            if debug:
                try:
                    dbg_name = f"debug_{subreddit_name}_{cur_start}_{page_count}.json"
                    with open(dbg_name, 'w', encoding='utf-8') as dbgf:
                        import json
                        json.dump(payload, dbgf)
                    print(f"DEBUG: dumped payload to {dbg_name}")
                except Exception:
                    pass
            data = payload.get('data', {})
            children = data.get('children', [])
            try:
                print(f"HTTP: fetched {len(children)} items from {url}")
            except Exception:
                pass
            if not children:
                break

            stop_window = False
            for item in children:
                d = item.get('data', {})
                c = d.get('created_utc')
                if c is None:
                    continue
                # if created after cur_end skip, if older than start_ts we will stop
                if c > cur_end:
                    continue
                if c < start_ts:
                    stop_window = True
                    break
                reddit_id = d.get('id')
                if not reddit_id or reddit_id in seen_ids:
                    continue
                seen_ids.add(reddit_id)
                any_found = True
                sub = SimpleNamespace(
                    id=reddit_id,
                    created_utc=c,
                    author=d.get('author'),
                    title=d.get('title'),
                    subreddit=SimpleNamespace(display_name=d.get('subreddit')),
                    permalink=d.get('permalink'),
                )
                try:
                    print(f"FOUND (anon): id={sub.id} title={getattr(sub,'title','')[:120]!r}")
                except Exception:
                    pass
                yield sub

            if stop_window:
                # We've reached older posts than the window start; finish this window
                break

            # compute next after token: prefer data.after, otherwise last child's fullname
            new_after = data.get('after')
            if not new_after:
                try:
                    last = children[-1].get('data', {})
                    new_after = last.get('name')
                    if new_after:
                        print(f"Pagination: no data.after; falling back to last.name={new_after}")
                except Exception:
                    new_after = None

            if not new_after or new_after in seen_afters or new_after == after:
                print(f"Pagination stopping: new_after={new_after} seen_before={new_after in seen_afters} same_as_prev={new_after==after}")
                # If the server won't advance with after tokens, try advancing the outer window
                try:
                    last = children[-1].get('data', {})
                    last_ts = last.get('created_utc')
                    if last_ts:
                        # move the overall scanning end to just before the last fetched item
                        cur_end = last_ts - 1
                        window_advanced = True
                        print(f"Advancing window to last child's ts-1 -> {cur_end}")
                except Exception:
                    pass
                break

            seen_afters.add(new_after)
            after = new_after
            page_count += len(children)
            time.sleep(sleep_between)

        # pause between windows and move to next older window (unless we already advanced using last child's timestamp)
        time.sleep(sleep_between)
        if window_advanced:
            # continue with updated cur_end
            continue
        cur_end = cur_start - 1

    # finished scanning range


def crawl_subreddit_new_anonymous(subreddit_name: str, query_phrase: str, sleep_between: float, debug: bool = False):
    """Page /r/<subreddit>/new.json from newest to oldest and yield items whose title contains query_phrase (case-insensitive).
    This is reliable for iterating the full subreddit history with anonymous requests.
    """
    headers = {'User-Agent': os.environ.get('REDDIT_USER_AGENT', 'backfill-script/0.1')}
    after = None
    seen_ids = set()
    page_count = 0
    lower_phrase = query_phrase.lower()
    page_num = 1
    while True:
        params = {'limit': '100'}
        if after:
            params['after'] = after
        url = f"https://old.reddit.com/r/{subreddit_name}/new.json"
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
        except Exception as e:
            print(f"HTTP error while crawling /r/{subreddit_name}/new.json: {e}")
            break

        if r.status_code == 429:
            ra = None
            try:
                ra = int(r.headers.get('Retry-After'))
            except Exception:
                ra = None
            if ra is None:
                ra = max(30, int(sleep_between * 5))
            print(f"Rate limited (429) for {url}; sleeping {ra}s")
            time.sleep(ra)
            continue
        if r.status_code != 200:
            print(f"Non-200 {r.status_code} for {url}")
            time.sleep(max(5, sleep_between))
            break

        payload = r.json()
        data = payload.get('data', {})
        children = data.get('children', [])
        fetched = len(children)
        matched = 0
        if debug:
            try:
                dbg_name = f"debug_new_{subreddit_name}_{page_count}.json"
                import json
                with open(dbg_name, 'w', encoding='utf-8') as dbgf:
                    json.dump(payload, dbgf)
                print(f"DEBUG: dumped payload to {dbg_name}")
            except Exception:
                pass

        if not children:
            break

        for item in children:
            d = item.get('data', {})
            reddit_id = d.get('id')
            if not reddit_id or reddit_id in seen_ids:
                continue
            title = (d.get('title') or '').strip()
            if lower_phrase in title.lower():
                seen_ids.add(reddit_id)
                sub = SimpleNamespace(
                    id=reddit_id,
                    created_utc=d.get('created_utc'),
                    author=d.get('author'),
                    title=title,
                    subreddit=SimpleNamespace(display_name=d.get('subreddit')),
                    permalink=d.get('permalink'),
                )
                try:
                    print(f"FOUND (new.json): id={sub.id} title={getattr(sub,'title','')[:120]!r}")
                except Exception:
                    pass
                matched += 1
                yield sub

        # per-page summary
        try:
            print(f"PAGE {page_num}: fetched={fetched} matched_new={matched} after={data.get('after')}")
        except Exception:
            pass
        page_num += 1

        new_after = data.get('after')
        if not new_after or new_after == after:
            print(f"new.json paging stopping: after={new_after}")
            break
        after = new_after
        page_count += len(children)
        time.sleep(sleep_between)


def crawl_subreddit_html(subreddit_name: str, query_phrase: str, sleep_between: float, debug: bool = False):
    """Crawl old.reddit.com HTML pages for the subreddit, following the 'next' button.
    This will visit listing pages (old.reddit.com/r/<subreddit>/) and follow pagination links
    until there are no more pages. It extracts post ids from permalinks or data-fullname and
    yields items whose title contains `query_phrase` (case-insensitive).
    """
    headers = {'User-Agent': os.environ.get('REDDIT_USER_AGENT', 'backfill-script/0.1')}
    url = f"https://old.reddit.com/r/{subreddit_name}/"
    seen_ids = set()
    lower_phrase = query_phrase.lower()
    import re

    # quick robots.txt check
    try:
        robots = requests.get('https://old.reddit.com/robots.txt', headers=headers, timeout=10).text
        # if /r/ is disallowed, abort
        for line in robots.splitlines():
            if line.strip().lower().startswith('disallow:'):
                path = line.split(':', 1)[1].strip()
                if path and path.startswith('/r/'):
                    print('robots.txt disallows /r/ paths; aborting HTML crawl')
                    return
    except Exception:
        # couldn't fetch robots; continue but be polite
        pass

    page_num = 1
    while url:
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            print(f"HTTP error while crawling {url}: {e}")
            break

        if r.status_code == 429:
            ra = None
            try:
                ra = int(r.headers.get('Retry-After'))
            except Exception:
                ra = None
            if ra is None:
                ra = max(30, int(sleep_between * 5))
            print(f"Rate limited (429) for {url}; sleeping {ra}s")
            time.sleep(ra)
            continue
        if r.status_code != 200:
            print(f"Non-200 {r.status_code} for {url}")
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        things = soup.find_all('div', class_='thing')
        fetched = len(things)
        matched = 0
        if debug:
            print(f"HTML: fetched {fetched} 'thing' elements from {url}")

        for div in things:
            # title link
            a = div.find('a', class_='title')
            if not a:
                continue
            title = (a.get_text() or '').strip()
            href = a.get('href') or ''
            if href.startswith('/'):
                permalink = 'https://reddit.com' + href
            else:
                permalink = href

            # try to extract id from permalink
            m = re.search(r'/comments/([0-9a-zA-Z]+)/', permalink)
            if m:
                reddit_id = m.group(1)
            else:
                fullname = div.get('data-fullname') or div.get('data-name') or ''
                if fullname.startswith('t3_'):
                    reddit_id = fullname.split('_', 1)[1]
                else:
                    continue

            if reddit_id in seen_ids:
                continue

            if lower_phrase in title.lower():
                # attempt to get timestamp
                ts = None
                try:
                    # some listings include data-timestamp in milliseconds
                    ts_attr = div.get('data-timestamp') or div.get('data-created-utc')
                    if ts_attr:
                        tval = int(ts_attr)
                        # if milliseconds, convert
                        if tval > 1e10:
                            ts = int(tval / 1000)
                        else:
                            ts = tval
                except Exception:
                    ts = None

                seen_ids.add(reddit_id)
                matched += 1
                sub = SimpleNamespace(
                    id=reddit_id,
                    created_utc=ts or 0,
                    author=div.get('data-author') or '',
                    title=title,
                    subreddit=SimpleNamespace(display_name=subreddit_name),
                    permalink=permalink,
                )
                try:
                    print(f"FOUND (html): id={sub.id} title={getattr(sub,'title','')[:120]!r}")
                except Exception:
                    pass
                yield sub

        # per-page summary
        try:
            print(f"PAGE {page_num}: fetched={fetched} matched_new={matched} url={url}")
        except Exception:
            pass
        page_num += 1

        # find next button
        next_btn = soup.find('span', class_='next-button')
        if next_btn:
            a = next_btn.find('a')
            if a and a.get('href'):
                url = a.get('href')
            else:
                url = None
        else:
            url = None

        time.sleep(sleep_between)


def crawl_user_html(username: str, query_phrase: str, sleep_between: float, debug: bool = False):
    """Crawl old.reddit.com user submitted pages (HTML) following 'next' links.
    Yields SimpleNamespace objects for posts whose title contains `query_phrase` (case-insensitive).
    All HTTP calls are printed to stdout. This function intentionally does not use any JSON endpoints.
    """
    headers = {'User-Agent': os.environ.get('REDDIT_USER_AGENT', 'backfill-script/0.1')}
    url = f"https://old.reddit.com/user/{username}/submitted/"
    seen_ids = set()
    lower_phrase = (query_phrase or '').lower()
    import re

    page_num = 1
    while url:
        try:
            # announce the call
            print(f"HTTP CALL: GET {url}")
            r = requests.get(url, headers=headers, timeout=30)
            print(f"HTTP STATUS: {r.status_code} for {url}")
        except Exception as e:
            print(f"HTTP error while crawling {url}: {e}")
            break

        if r.status_code == 429:
            ra = None
            try:
                ra = int(r.headers.get('Retry-After'))
            except Exception:
                ra = None
            if ra is None:
                ra = max(30, int(sleep_between * 5))
            print(f"Rate limited (429) for {url}; sleeping {ra}s")
            time.sleep(ra)
            continue
        if r.status_code != 200:
            print(f"Non-200 {r.status_code} for {url}")
            break

        soup = BeautifulSoup(r.text, 'html.parser')
        things = soup.find_all('div', class_='thing')
        fetched = len(things)
        matched = 0
        if debug:
            print(f"HTML: fetched {fetched} 'thing' elements from {url}")

        for div in things:
            a = div.find('a', class_='title')
            if not a:
                continue
            title = (a.get_text() or '').strip()
            href = a.get('href') or ''
            if href.startswith('/'):
                permalink = 'https://reddit.com' + href
            else:
                permalink = href

            # extract id from permalink
            m = re.search(r'/comments/([0-9a-zA-Z]+)/', permalink)
            if m:
                reddit_id = m.group(1)
            else:
                fullname = div.get('data-fullname') or div.get('data-name') or ''
                if fullname.startswith('t3_'):
                    reddit_id = fullname.split('_', 1)[1]
                else:
                    continue

            if reddit_id in seen_ids:
                continue

            if lower_phrase in title.lower():
                # try to get timestamp
                ts = None
                try:
                    ts_attr = div.get('data-timestamp') or div.get('data-created-utc')
                    if ts_attr:
                        tval = int(ts_attr)
                        if tval > 1e10:
                            ts = int(tval / 1000)
                        else:
                            ts = tval
                except Exception:
                    ts = None

                seen_ids.add(reddit_id)
                matched += 1
                sub = SimpleNamespace(
                    id=reddit_id,
                    created_utc=ts or 0,
                    author=div.get('data-author') or '',
                    title=title,
                    subreddit=SimpleNamespace(display_name=''),
                    permalink=permalink,
                )
                try:
                    print(f"FOUND (user-html): id={sub.id} title={getattr(sub,'title','')[:120]!r}")
                except Exception:
                    pass
                yield sub

        try:
            print(f"PAGE {page_num}: fetched={fetched} matched_new={matched} url={url}")
        except Exception:
            pass
        page_num += 1

        next_btn = soup.find('span', class_='next-button')
        if next_btn:
            a = next_btn.find('a')
            if a and a.get('href'):
                url = a.get('href')
            else:
                url = None
        else:
            url = None

        time.sleep(sleep_between)


def write_csv_row(writer, fh, sub, simple: bool = False, source='reddit'):
    """Write a row to CSV and flush immediately. If `simple` write only id,title,permalink."""
    if simple:
        row = {
            'id': sub.id,
            'title': getattr(sub, 'title', '')[:1000],
            'permalink': ("https://reddit.com" + getattr(sub, 'permalink', '')) if getattr(sub, 'permalink', None) else '',
        }
    else:
        row = {
            'id': sub.id,
            'created_utc': int(getattr(sub, 'created_utc', 0)) if hasattr(sub, 'created_utc') else 0,
            'author': str(sub.author) if getattr(sub, 'author', None) else '',
            'title': getattr(sub, 'title', '')[:1000],
            'subreddit': getattr(sub, 'subreddit', '').display_name if getattr(sub, 'subreddit', None) else '',
            'permalink': ("https://reddit.com" + getattr(sub, 'permalink', '')) if getattr(sub, 'permalink', None) else '',
            'source': source,
        }
    writer.writerow(row)
    try:
        fh.flush()
        os.fsync(fh.fileno())
    except Exception:
        pass
    # verbose stdout
    try:
        if simple:
            print(f"WROTE: id={row['id']} url={row['permalink']}")
        else:
            print(f"WROTE: id={row['id']} title={row['title'][:80]!r}")
    except Exception:
        pass


def main():

    # Custom: Crawl /r/wowthissubexists for posts by AutoModerator containing 'Fap Friday' in title
    subreddit_name = 'wowthissubexists'
    author_name = 'AutoModerator'
    phrase = 'Fap Friday'
    output_file = 'backfill_AutoModerator_FapFriday.csv'
    sleep_between = 7.5  # stay under 8 calls/min
    years = 11
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=365 * years)
    end_ts = epoch(end_dt)
    start_ts = epoch(start_dt)
    query_base = f'author:{author_name} AND title:"{phrase}"'
    fieldnames = ['reddit_post_id', 'title', 'url', 'created_utc']

    print(f"Crawling /r/{subreddit_name} for posts by {author_name} containing '{phrase}' in title...")
    print(f"Output CSV: {output_file}")
    print(f"Date range: {datetime.fromtimestamp(start_ts).date()} to {datetime.fromtimestamp(end_ts).date()}")

    reddit = make_reddit()
    with open(output_file, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        count = 0
        for sub in search_windows(reddit, subreddit_name, query_base, start_ts, end_ts, 30, sleep_between, site_wide=False, debug=False):
            row = {
                'reddit_post_id': sub.id,
                'title': getattr(sub, 'title', ''),
                'url': getattr(sub, 'permalink', ''),
                'created_utc': int(getattr(sub, 'created_utc', 0)) if getattr(sub, 'created_utc', None) else 0,
            }
            writer.writerow(row)
            try:
                fh.flush()
                os.fsync(fh.fileno())
            except Exception:
                pass
            print(f"WROTE: id={row['reddit_post_id']} title={row['title'][:80]!r} url={row['url']}")
            count += 1
    print(f"Done. {count} results written to {output_file}")


if __name__ == '__main__':
    main()
