import os
import sys
import logging
import argparse
import shelve
from threading import Lock
from pathlib import Path
from itertools import chain
from multiprocessing.pool import ThreadPool

import jmespath
from babelfish import Language
from tabulate import tabulate
from subliminal import scan_videos, save_subtitles, download_best_subtitles, cache
from imdb import IMDb, IMDbError

DEFAULT_PLACES = ['~/Downloads', '~/.aMule/Incoming']


def thread_map(function, iterable):
    return ThreadPool(POOL_SIZE).map(function, iterable)


def get_videos(dirs):
    videos_key = ' '.join(sorted(map(str, dirs)))
    with shelve.open('.videos.cache.shelve') as db:
        videos = db.get(videos_key)
    if videos:
        return videos

    videos = unique_videos(
        chain.from_iterable(thread_map(scan_videos, map(str, dirs)))
    )

    with shelve.open('.videos.cache.shelve') as db:
        db[videos_key] = videos

    return videos

def unique_videos(videos):
    unique = {}
    for video in videos:
        other = unique.get(video.title)
        if other is None:
            unique[video.title] = video
        else:
            unique[video.title] = max(video, other, key=video_size)
    return set(unique.values())


def video_size(video):
    return Path(video.name).stat().st_size


def get_imdb_metadata(videos):
    db = IMDb()

    def _search_movie(video):
        year = str(video.year) if video.year else ''
        title = f'{video.title} {year}'.strip()
        return search_movie(db, title)

    for video, result in zip(videos, thread_map(_search_movie, videos)):
        if result:
            yield video, result
        #else:
        #    print(f'No results for {video.title}')

meta_cache_lock = Lock()


def get_cached_meta(title):
    with meta_cache_lock:
        with shelve.open('.meta.cache.shelve') as db:
            return db.get(str(title))


def save_cached_meta(title, meta):
    with meta_cache_lock:
        with shelve.open('.meta.cache.shelve') as db:
            db[title] = meta


def search_movie(db, title):
    meta = get_cached_meta(title)
    if meta:
        return meta

    try:
        search_result = db.search_movie(title)
    except Exception:
        return None

    if not search_result:
        return None

    meta = search_result[0]
    try:
        movie = db.get_movie(meta.movieID)
    except Exception:
        return None

    save_cached_meta(title, movie)
    return movie


def download_subtitles(videos):
    subtitles = download_best_subtitles(
        videos, {Language('eng'), Language('spa')}
    )
    for video in videos:
        save_subtitles(video, subtitles[video])


def print_metadata(metadata):
    metadata = list(metadata)
    headers = [
        'img_url', 'title', 'year', 'country',
        'directors', 'rating', 'summary', 'path'
    ]
    entries = []
    get_rating = jmespath.compile('demographics."non us users".rating').search
    for video, meta in metadata:
        title = meta['title']
        url = f'https://www.imdb.com/title/tt{meta.movieID}'
        img_url = meta.get('cover url')
        entries.append({
            'title': f"[{title}]({url})",
            'year': meta.get('year') or video.year or '',
            'url': url,
            'path': f'[path]({video.name})',
            'img_url': f"![]({img_url})",
            'rating': meta.get('rating'),
            'directors': ' '.join(
                d['name'] for d in meta.get('directors') or []
            ),
            'country': ' '.join(meta.get('countries') or []),
            'summary': meta.get('plot outline') or meta.get('plot', '')[:300],
        })
        #print(f'- {meta["kind"]}: {meta["long imdb title"]} ')
    table = [[e.get(h) for h in headers] for e in entries]
    print(tabulate(table, headers, tablefmt='pipe'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', default=10, type=int)
    parser.add_argument('--metadata', action='store_true', default=False)
    parser.add_argument('--subtitles', action='store_true', default=False)
    parser.add_argument('--dirs', default=','.join(DEFAULT_PLACES))
    parser.add_argument('--dir', action='append', default=[])
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format='%(levelname)s: %(name)s %(message)s'
    )
    cache.region.configure(
        'dogpile.cache.dbm', arguments={'filename': '/tmp/cachefile.dbm'}
    )

    global POOL_SIZE
    POOL_SIZE = args.workers
    if args.dirs:
        args.dirs = [Path(d.strip()) for d in args.dirs.split(',')]
    if args.dir:
        args.dirs.extend(Path(d.strip()) for d in args.dir)
    args.dirs = [d.expanduser().absolute() for d in args.dirs]

    logging.info('Getting videos %s', args.dirs)
    videos = get_videos(args.dirs)
    logging.info(
        'Found %d videos in %d locations', len(videos), len(args.dirs)
    )

    metadata = []
    if args.metadata:
        logging.info('Fetching imdb metadata for %d videos', len(videos))
        metadata = sorted(
            get_imdb_metadata(videos),
            key=lambda e: float(e[1].get('rating') or 0),
            reverse=True,
        )
        print_metadata(metadata)
    if args.subtitles:
        logging.info('Fetching subtitles for %d videos', len(videos))
        download_subtitles(videos)
