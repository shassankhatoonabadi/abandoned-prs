from github import BadCredentialsException, RateLimitExceededException, UnknownObjectException
from joblib import Parallel, delayed
from pandas import DataFrame

from common import TOKENS, cleanup, github, initialize, logger, paths, refresh

initialize()
log = logger(__file__, modules={"urllib3": "ERROR"})


def fetch_projects():
    token, client = github()
    while True:
        try:
            log.info("Fetching list of projects")
            projects = [
                project.full_name.lower() for project in client.search_repositories("stars:>15000", sort="stars")
            ]
        except (BadCredentialsException, RateLimitExceededException):
            token, client = github(token)
        except Exception as exception:
            log.error(f"Failed fetching list of projects due to {exception}")
        else:
            break
    github(token, done=True)
    return projects


def fetch_metadata(project):
    metadata = {"project": project, "pulls": None, "stars": None}
    token, client = github()
    while True:
        try:
            log.info(f"{project}: Fetching metadata")
            repository = client.get_repo(project)
            metadata.update(
                {
                    "project": repository.full_name.lower(),
                    "pulls": repository.get_pulls(state="all").totalCount,
                    "stars": repository.watchers,
                }
            )
        except (BadCredentialsException, RateLimitExceededException):
            token, client = github(token)
        except UnknownObjectException:
            log.warning(f"{project}: Project does not exist")
            break
        except Exception as exception:
            log.error(f"{project}: Failed fetching metadata due to {exception}")
        else:
            break
    github(token, done=True)
    return metadata


def export_projects(metadata):
    DataFrame(metadata).sort_values(["pulls", "stars"], ascending=False).drop_duplicates("project").to_csv(
        paths("projects"), index=False
    )


def main():
    if cleanup("projects", refresh()):
        with Parallel(n_jobs=len(TOKENS), prefer="threads") as parallel:
            export_projects(parallel(delayed(fetch_metadata)(project) for project in fetch_projects()))
    else:
        print("Skip fetching projects")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop fetching projects")
        exit(1)
