from argparse import ArgumentParser
from functools import reduce
from json import dumps, loads
from logging import getLogger
from logging.config import dictConfig
from os import chdir
from pathlib import Path
from queue import Queue
from sys import maxsize, setrecursionlimit, version_info

from github import BadCredentialsException, Github, GithubObject, RateLimitExceededException
from pandas import Timestamp, read_csv
from sqlitedict import SqliteDict
from urllib3.util.retry import Retry

log = getLogger(__name__)
DATE = Timestamp(2020, 5, 30)
KEYWORDS = [
    "abandon",
    "stale",
    "any update",
    "lack of update",
    "no update",
    "inactive",
    "inactivity",
    "lack of activity",
    "no activity",
    "not active",
    "lack of reply",
    "no reply",
    "lack of response",
    "no response",
]
TOKENS = {}
tokens = Queue()
for token in TOKENS:
    tokens.put(token)


@property
def raw_data(self):
    return self._rawData


GithubObject.GithubObject.data = raw_data


def initialize(directory=None):
    if not (version_info[0:2] == (3, 9) and maxsize > 2**32):
        raise RuntimeError("Python 3.9 (64-bit) is required")
    setrecursionlimit(1_000_000)
    if directory is None:
        directory = paths("data")
    directory = Path(__file__).parent / directory
    directory.mkdir(parents=True, exist_ok=True)
    chdir(directory)


def logger(name, level="INFO", modules=None):
    name = Path(name).stem
    dictConfig(
        {
            "version": 1,
            "formatters": {
                "file": {
                    "format": "{asctime}\t{levelname}\t{name}\t{message}",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "style": "{",
                },
                "stream": {
                    "()": "colorlog.ColoredFormatter",
                    "format": "{blue}{asctime}\t{name}\t{message_log_color}{message}",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "style": "{",
                    "secondary_log_colors": {
                        "message": {
                            "DEBUG": "cyan",
                            "INFO": "green",
                            "WARNING": "yellow",
                            "ERROR": "red",
                            "CRITICAL": "bold_red",
                        }
                    },
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "file",
                    "filename": f"{name}.log",
                },
                "stream": {
                    "class": "colorlog.StreamHandler",
                    "formatter": "stream",
                },
            },
            "root": {
                "level": level,
                "handlers": ["file", "stream"],
            },
            "disable_existing_loggers": False,
        }
    )
    if modules is not None:
        for module, level in modules.items():
            getLogger(module).setLevel(level)
    return getLogger(name)


def github(token=None, done=False):
    if token is not None:
        tokens.put(token)
    if not done:
        while True:
            try:
                token = tokens.get()
                client = Github(
                    token,
                    timeout=20,
                    per_page=100,
                    retry=Retry(total=None, status=10, backoff_factor=1, status_forcelist=[500, 502, 503, 504]),
                )
                remaining, limit = client.rate_limiting
                if limit < 5000:
                    raise BadCredentialsException(401, f"Token {token} is blocked", headers=None)
            except BadCredentialsException:
                log.warning(f"Token {token} is not valid")
            except Exception as exception:
                if not isinstance(exception, RateLimitExceededException):
                    log.error(f"Token {token} is not working due to {exception}")
                tokens.put(token)
            else:
                if remaining > TOKENS[token]:
                    break
                else:
                    tokens.put(token)
        return token, client


def persist(file):
    def encode(data):
        return dumps(data, ensure_ascii=False, separators=(",", ":"))

    def decode(data):
        return loads(data)

    return SqliteDict(file, tablename="data", autocommit=True, encode=encode, decode=decode)


def lookup(attributes, json):
    if not isinstance(attributes, list):
        attributes = [attributes]
    for attribute in attributes:
        if (
            value := reduce(
                lambda dictionary, key: dictionary.get(key) if dictionary else None, attribute.split("."), json
            )
        ) not in [None, ""]:
            return value


def paths(file, project=None):
    if project is not None:
        project = project.replace("/", "_").lower()
    directory = f"{project}/"
    files = {
        # Working directory
        "data": "data/",
        # Generated in fetch_projects.py
        "projects": "projects.csv",
        # Generated in collect_data.py
        "directory": directory,
        "checkpoint": directory + f"{project}_checkpoint.db",
        "pulls_raw": directory + f"{project}_pulls.db",
        "timelines_raw": directory + f"{project}_timelines.db",
        "commits": directory + f"{project}_commits.db",
        "files": directory + f"{project}_files.db",
        "metadata": directory + f"{project}.db",
        # Generated in preprocess_data.py
        "timelines_fixed": directory + f"{project}_timelines_fixed.db",
        "timelines_preprocessed": directory + f"{project}_timelines.csv",
        "pulls_preprocessed": directory + f"{project}_pulls.csv",
        # Generated in process_data.py
        "dataframe": directory + f"{project}_dataframe.csv",
        # Generated in postprocess_data.py
        "statistics": "statistics.csv",
        "dataset": directory + f"{project}_dataset.csv",
        "sample": directory + f"{project}_sample.csv",
        # Generated in analyze_inactivity.py
        "inactivity": "inactivity.csv",
        # Generated in prelabel_data.py
        "prelabeling": "prelabeling.csv",
        # Generated in label_data.py
        "labeling": "labeling.csv",
        # Generated after labeling
        "labels": "labels.xlsx",
        # Generated in calculate_agreement.py
        "agreement": "agreement.csv",
        # Generated in extract_developers.py
        "developers": "developers.csv",
        # Generated after survey
        "responses": f"{project}.csv",
        # Generated in analyze_survey.py
        "survey": "survey.xlsx",
        # Generated in measure_features.py
        "features": directory + f"{project}_features.csv",
        # Generated in build_deeplearning.py
        "deeplearning": "deeplearning.csv",
    }
    return Path(files[file])


def refresh():
    parser = ArgumentParser()
    parser.add_argument("-y", action="store_true", help="force fresh start")
    parser.add_argument("-n", action="store_true", help="do not force fresh start")
    if (parser := parser.parse_args()).y:
        return True
    elif parser.n:
        return False


def cleanup(files, fresh=None, project=None):
    if not isinstance(files, list):
        files = [files]
    files = [paths(file, project) for file in files]
    if (exists := any([file.exists() for file in files])) and fresh is None:
        message = "Do you want to force fresh start? [y/n] "
        if project is not None:
            message = f"{project}: {message}"
        while True:
            if (fresh := input(message).lower()) in ["y", "n"]:
                fresh = True if fresh == "y" else False
                break
    if fresh:
        for file in files:
            file.unlink(missing_ok=True)
    return True if fresh or not exists else False


def exist(files, project, exclude=None):
    if not isinstance(files, list):
        files = [files]
    if exclude is None:
        exclude = []
    elif not isinstance(exclude, list):
        exclude = [exclude]
    return all([paths(file, project).exists() for file in files]) and not any(
        [paths(file, project).exists() for file in exclude]
    )


def tocollect():
    if not (projects := paths("projects")).exists():
        raise RuntimeError("List of projects is missing")
    return read_csv(projects, usecols=["project"]).squeeze("columns").dropna()


def collected():
    return [
        project
        for project in tocollect()
        if exist(["pulls_raw", "timelines_raw", "commits", "files", "metadata"], project, exclude="checkpoint")
    ]


def preprocessed():
    return [
        project
        for project in collected()
        if exist(["timelines_fixed", "timelines_preprocessed", "pulls_preprocessed"], project)
    ]


def processed():
    return [project for project in preprocessed() if exist("dataframe", project)]


def postprocessed():
    return [project for project in processed() if exist(["dataset", "sample"], project)]


def measured():
    return [project for project in postprocessed() if exist("features", project)]
