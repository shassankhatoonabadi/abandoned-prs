from csv import QUOTE_ALL

from joblib import Parallel, delayed
from pandas import DataFrame

from common import cleanup, collected, initialize, logger, lookup, paths, persist, refresh

initialize()


def enrich_committed(timeline, commits):
    events = []
    for event in timeline:
        if event["event"] == "committed":
            event["author"]["login"] = lookup("author.login", commits[event["sha"]])
        events.append(event)
    return events


def enrich_referenced(timeline):
    events = []
    for event in timeline:
        if event["event"] == "referenced":
            event["referenced"] = event["url"].split("/")[4:6] == event["commit_url"].split("/")[4:6]
        events.append(event)
    return events


def unpack_line_or_commit_commented(timeline):
    events = []
    for event in timeline:
        if event["event"] in ["line-commented", "commit-commented"]:
            for comment in event["comments"]:
                events.append({"event": event["event"], **comment})
        else:
            events.append(event)
    return events


def insert_pulled(timeline, pull):
    return [{"event": "pulled", **pull}, *timeline]


def identify_actor(timeline):
    events = []
    for event in timeline:
        actor = lookup(["actor.login", "user.login", "author.login"], event)
        event["actor"] = actor if actor is not None else "ghost"
        events.append(event)
    return events


def identify_time(timeline):
    events = []
    for event in timeline:
        event["time"] = lookup(["created_at", "committer.date", "submitted_at"], event)
        events.append(event)
    return events


def add_pull_and_event_number(timeline):
    events = []
    pull_number = timeline[0]["number"]
    for event_number, event in enumerate(sorted(timeline, key=lambda event: event["time"])):
        event["pull_number"] = pull_number
        event["event_number"] = event_number
        events.append(event)
    return events


def fix_timeline(timeline, pull, commits):
    timeline = enrich_committed(timeline, commits)
    timeline = enrich_referenced(timeline)
    timeline = unpack_line_or_commit_commented(timeline)
    timeline = insert_pulled(timeline, pull)
    timeline = identify_actor(timeline)
    timeline = identify_time(timeline)
    return add_pull_and_event_number(timeline)


def fix_timelines(project, timelines, pulls, commits):
    fixed = persist(paths("timelines_fixed", project))
    for pull in pulls:
        fixed[pull] = fix_timeline(timelines[pull], pulls[pull], commits[pull])
    return fixed


def filter_timelines(timelines):
    rows = []
    for timeline in timelines.values():
        for event in timeline:
            row = {}
            for column in [
                "pull_number",
                "event_number",
                "event",
                "actor",
                "author_association",
                "author.name",
                "author.email",
                "time",
                "merged_at",
                "state",
                "commit_id",
                "referenced",
                "body",
            ]:
                row[column] = lookup(column, event)
            rows.append(row)
    return rows


def filter_pulls(pulls):
    rows = []
    for pull in pulls.values():
        row = {}
        for column in ["number", "html_url", "title", "body"]:
            row[column] = lookup(column, pull)
        rows.append(row)
    return rows


def export_timelines(project, timelines):
    DataFrame(timelines).sort_values(["pull_number", "event_number"]).to_csv(
        paths("timelines_preprocessed", project), index=False, quoting=QUOTE_ALL
    )


def export_pulls(project, pulls):
    DataFrame(pulls).sort_values("number").to_csv(paths("pulls_preprocessed", project), index=False, quoting=QUOTE_ALL)


def preprocess_data(project):
    log = logger(__file__, modules={"sqlitedict": "WARNING"})
    log.info(f"{project}: Preprocessing data")
    timelines = persist(paths("timelines_raw", project))
    pulls = persist(paths("pulls_raw", project))
    commits = persist(paths("commits", project))
    export_timelines(project, filter_timelines(fix_timelines(project, timelines, pulls, commits)))
    export_pulls(project, filter_pulls(pulls))


def main():
    projects = []
    for project in collected():
        if cleanup(["timelines_fixed", "timelines_preprocessed", "pulls_preprocessed"], refresh(), project):
            projects.append(project)
        else:
            print(f"Skip preprocessing data for project {project}")
    with Parallel(n_jobs=-1) as parallel:
        parallel(delayed(preprocess_data)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop preprocessing data")
        exit(1)
