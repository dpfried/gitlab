import csv
import json
from collections import Counter

from scrape import csv_columns

def read_file(fname):
    with open(fname, 'r') as f:
        csv_reader = iter(csv.DictReader(f, csv_columns))
        next(csv_reader)
        yield from csv_reader


def aggregate_stats(filenames):
    license_count = Counter()
    repo_urls = set()

    majority_language_counts = Counter()
    license_and_language_counts = Counter()

    no_python_count = 0

    for fname in filenames:
        for record in read_file(fname):
            if record['url'] in repo_urls:
                continue
            repo_urls.add(record['url'])
            license = record['license']
            language_percentages = json.loads(record['languages'])
            majority_language = max(language_percentages.items(), key=lambda t: t[1])[0]
            if 'Python' not in language_percentages:
                no_python_count += 1
            license_count[license] += 1
            majority_language_counts[majority_language] += 1
            license_and_language_counts[(license, majority_language)] += 1

    return {
        'num_repos': len(repo_urls),
        'license_counts': license_count,
        'majority_language_counts': majority_language_counts,
        'license_and_language_counts': license_and_language_counts,
    }

if __name__ == "__main__":
    import glob
    filenames = glob.glob("scrapes/*.csv")
    aggregated_stats = aggregate_stats(filenames)

    open_source_licenses = {'mit', 'apache-2.0', 'bsd-3-clause', 'bsd-2-clause'}

    num_repos = aggregated_stats['num_repos']

    open_source = sum(v for k, v in aggregated_stats['license_counts'].items()
                      if k in open_source_licenses)

    print(f"{open_source} / {num_repos} ({open_source/num_repos*100:.2f}%) open source")
    mostly_python_and_open_source = sum(v for (license, lang), v in aggregated_stats['license_and_language_counts'].items()
                                        if license in open_source_licenses and lang == 'Python')
    print(f"{mostly_python_and_open_source} / {num_repos} ({mostly_python_and_open_source/num_repos*100:.2f}%) open source and mostly Python")

