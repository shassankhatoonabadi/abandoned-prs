from github import BadCredentialsException, GithubException, RateLimitExceededException, UnknownObjectException
from joblib import Parallel, delayed
from requests.exceptions import RetryError

from common import TOKENS, cleanup, github, initialize, logger, paths, persist, refresh, tocollect

initialize()
log = logger(__file__, modules={"sqlitedict": "WARNING", "urllib3": "ERROR"})


def delete_pull(databases, pull):
    if not isinstance(databases, list):
        databases = [databases]
    for database in databases:
        try:
            del database[pull]
        except KeyError:
            pass


def collect_data(project):
    paths("directory", project).mkdir(parents=True, exist_ok=True)
    checkpoint = persist(paths("checkpoint", project))
    pulls = persist(paths("pulls_raw", project))
    timelines = persist(paths("timelines_raw", project))
    commits = persist(paths("commits", project))
    files = persist(paths("files", project))
    metadata = persist(paths("metadata", project))
    if checkpoint.get("last") is None:
        checkpoint["last"] = 0
        checkpoint["exclude"] = []
    else:
        log.info(f"{project}: Last collected data is for pull request {checkpoint.get('pull')}")
    token, client = github()
    while True:
        try:
            log.info(f"{project}: Collecting list of pull requests")
            repository = client.get_repo(project)
            for pull in repository.get_pulls(state="all", direction="asc")[checkpoint["last"] :]:
                if client.rate_limiting[0] <= TOKENS[token]:
                    raise RateLimitExceededException(403, f"Reached custom rate limit for token {token}", headers=None)
                if (pull_number := pull.number) in checkpoint["exclude"]:
                    log.info(f"{project}: Deleting data for pull request {pull_number}")
                    delete_pull([pulls, timelines, commits, files], pull_number)
                else:
                    log.info(f"{project}: Collecting data for pull request {pull_number}")
                    pulls[pull_number] = pull.data
                    timelines[pull_number] = [event.data for event in repository.get_issue(pull_number).get_timeline()]
                    commits[pull_number] = {commit.data["sha"]: commit.data for commit in pull.get_commits()}
                    files[pull_number] = {file.data["sha"]: file.data for file in pull.get_files()}
                checkpoint["pull"] = pull_number
                checkpoint["last"] += 1
        except (BadCredentialsException, RateLimitExceededException):
            token, client = github(token)
        except UnknownObjectException:
            log.warning(f"{project}: Project does not exist")
            break
        except Exception as exception:
            if (isinstance(exception, GithubException) and exception.status == 422) or isinstance(
                exception, RetryError
            ):
                log.warning(f"{project}: Skip collecting data for pull request {pull_number} due to {exception}")
                checkpoint["exclude"] = [*checkpoint["exclude"], pull_number]
            else:
                log.error(f"{project}: Failed collecting data due to {exception}")
        else:
            metadata.update(repository.data)
            checkpoint.terminate()
            log.info(f"{project}: Finished collecting data")
            break
    github(token, done=True)


def main():
    projects = []
    for project in tocollect():
        if (
            cleanup(["checkpoint", "pulls_raw", "timelines_raw", "commits", "files", "metadata"], refresh(), project)
            or paths("checkpoint", project).exists()
        ):
            projects.append(project)
        else:
            print(f"Skip collecting data for project {project}")
    with Parallel(n_jobs=len(TOKENS), prefer="threads") as parallel:
        parallel(delayed(collect_data)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop collecting data")
        exit(1)
