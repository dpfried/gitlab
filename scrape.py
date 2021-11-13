import sys
import json
import time
import tqdm
import pprint
from urllib.request import Request, urlopen
from collections import Counter

import datetime

import csv

import gitlab
import dateutil
import dateutil.parser
import argparse

APPROX_MAX_ID = 32_000_000

csv_columns = ['id', 'url', 'stars', 'license', 'main_language', 'is_fork', 'last_activity_at', 'tags', 'languages']

if __name__ == "__main__":
    print(' '.join(sys.argv))
    parser = argparse.ArgumentParser()
    parser.add_argument("out_fname")
    parser.add_argument("private_token")
    parser.add_argument("--descending", action='store_true')
    parser.add_argument("--start_id", type=int)
    parser.add_argument("--shard", type=int)
    parser.add_argument("--num_shards", type=int, default=10)
    parser.add_argument("--language", default='Python')

    args = parser.parse_args()
    pprint.pprint(vars(args))

    private_token = args.private_token
    out_fname = args.out_fname

    gl = gitlab.Gitlab('http://gitlab.com', private_token=private_token)
    gl.auth()

    language_counts = Counter()
    star_counts = Counter()
    license_counts = Counter()
    is_fork_counts = Counter()

    records = []

    limit = 100
    #it = gl.projects.list(per_page=10, page=1, with_programming_language='python', license=True, order_by='last_activity_at', sort='desc')
    #it = gl.projects.list(all=True, per_page=limit, with_programming_language='python', license=True, as_list=False, order_by="last_activity_at", sort="desc")
    #it = tqdm.tqdm(it, ncols=80)

    csv_file = open(out_fname, 'w')
    csv_writer = csv.DictWriter(csv_file, csv_columns)
    csv_writer.writeheader()

    repo_count = 0
    usable_repos = 0

    if args.shard is not None:
        assert not args.descending
        start_id = (APPROX_MAX_ID // args.num_shards) * args.shard
        end_id = (APPROX_MAX_ID // args.num_shards) * (args.shard + 1)
    else:
        start_id = None
        end_id = None
    if args.start_id:
        start_id = args.start_id

    print(f"scraping ids {start_id} -- {end_id}")

    first = True

    while True:
        params = dict(page=1, per_page=limit, with_programming_language=args.language.lower(), license=True, as_list=True, order_by="id")
        if not args.descending:
            params['sort'] = "asc"
            if start_id is not None:
                params['id_after'] = str(start_id)
        else:
            params['sort'] = "desc"
            if start_id is not None:
                params['id_before'] = int(start_id) - 1
        first = False
        try:
            projects = gl.projects.list(**params)
        except:
            start_id += 1
            continue

        if not bool(projects):
            break
        for project in projects:
            requests_remaining = 0
            try:
                url = f"https://gitlab.com/api/v4/projects/{project.id}?license=true"
                request = Request(url, headers={'PRIVATE-TOKEN': private_token})
                response = urlopen(request)
                response_payload = response.read()
                data = json.loads(response_payload.decode('utf-8'))

                requests_remaining = int(response.headers.get('RateLimit-Remaining'))
                rate_reset_time = dateutil.parser.parse(response.headers.get('RateLimit-ResetTime'))

                project_url = project.web_url
                if data['license'] is not None:
                    license = data['license']['key']
                else:
                    license = None
                stars = data['star_count']
                tag_list = data['tag_list']
                languages = project.languages()
                if bool(languages):
                    main_language = max(languages.items(), key=lambda t: t[1])[0]
                else:
                    main_language = None

                is_fork = 'forked_from_project' in data and bool(data['forked_from_project'])

                start_id = project.id + 1

                record = {
                    'id': project.id,
                    'url': project_url,
                    'stars': stars,
                    'tags': tag_list,
                    'license': license,
                    'main_language': main_language,
                    'is_fork': is_fork,
                    'last_activity_at': data['last_activity_at'],
                    'languages': json.dumps(languages)
                }

                records.append(record)
                csv_writer.writerows([record])

                language_counts[main_language] += 1
                star_counts[stars] += 1
                license_counts[license] += 1
                is_fork_counts[is_fork] += 1

                is_usable = (license in {'mit', 'apache-2.0', 'bsd-2-clause', 'bsd-3-clause'}) and (not is_fork) and (main_language == args.language)
                if is_usable:
                    usable_repos += 1
                repo_count += 1

                print(f"{repo_count:_}\t{record}")

            except Exception as e:
                print(e)

            if (repo_count) % 100 == 0:
                print("**printing stats**")
                print(f"{usable_repos:_} / {repo_count:_} usable repos")
                print(language_counts.most_common())
                print(license_counts.most_common())
                print(is_fork_counts.most_common())
                print()
                csv_file.flush()

            if end_id is not None and project.id > end_id:
                break

            # print(f"{project_url}")
            # print(f"requests_remaining: {requests_remaining}")

            try:
                if requests_remaining <= 2:
                    sleep_seconds = (rate_reset_time - datetime.datetime.now(dateutil.tz.UTC)).seconds
                    print(f"out of requests; sleeping for {sleep_seconds} seconds")
                    time.sleep(sleep_seconds+2)
            except Exception as e:
                print(e)
                print("sleeping for 60 seconds")
                time.sleep(60)
    print("scraping complete")
