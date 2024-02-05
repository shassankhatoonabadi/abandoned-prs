from csv import QUOTE_ALL

from joblib import Parallel, cpu_count, delayed
from numpy import arange, array_split
from pandas import concat, notna, read_csv, to_datetime

from common import DATE, KEYWORDS, cleanup, initialize, logger, paths, preprocessed, refresh

initialize()
log = logger(__file__)


def import_timelines(project):
    file = paths("timelines_preprocessed", project)
    return read_csv(
        file,
        index_col=["pull_number", "event_number"],
        usecols=[column for column in read_csv(file, nrows=0) if column not in ["author.name", "author.email"]],
        dtype={
            "event": "category",
            "actor": "category",
            "author_association": "category",
            "state": "category",
            "commit_id": "category",
            "referenced": "boolean",
            "body": "string",
        },
        parse_dates=["time", "merged_at"],
        date_parser=lambda time: to_datetime(time, infer_datetime_format=True).tz_convert(tz=None),
        quoting=QUOTE_ALL,
    )


def chunk_timelines(project):
    groups = [group[1] for group in import_timelines(project).groupby("pull_number")]
    for indices in array_split(arange(len(groups)), cpu_count()):
        yield concat([groups[index] for index in indices])


def fill_status(timelines):
    def find_status(timeline):
        pulled = timeline.query("event == 'pulled'")
        closed = timeline.query("event == 'closed'")
        timeline["opened_at"] = pulled["time"].iat[0]
        timeline = timeline.assign(closed_at=None, merged_at=None, open=False, closed=False, merged=False)
        if pulled["state"].iat[0] == "closed":
            if notna(merged_time := pulled["merged_time"].iat[0]):
                timeline["merged"] = True
                timeline["merged_at"] = merged_time
            elif not closed.empty and notna(closed["commit_id"].iat[-1]):
                timeline["merged"] = True
                timeline["merged_at"] = closed["time"].iat[-1]
            elif not (referenced := timeline.query("referenced")).empty:
                timeline["merged"] = True
                timeline["merged_at"] = referenced["time"].iat[0]
            else:
                timeline["closed"] = True
                if not closed.empty:
                    timeline["closed_at"] = closed["time"].iat[-1]
        else:
            timeline["open"] = True
        return timeline[["opened_at", "closed_at", "merged_at", "open", "closed", "merged"]]

    timelines = timelines.rename(columns={"merged_at": "merged_time"})
    timelines[["opened_at", "closed_at", "merged_at", "open", "closed", "merged"]] = (
        timelines[["event", "time", "merged_time", "state", "commit_id", "referenced"]]
        .groupby("pull_number", group_keys=False)
        .apply(find_status)
    )
    return timelines.drop(columns=["merged_time", "state", "commit_id", "referenced"]).astype(
        {"closed_at": "datetime64[ns]", "merged_at": "datetime64[ns]"}
    )


def fill_contributor(timelines):
    def find_contributor(timeline):
        return timeline["actor"] == timeline.query("event == 'pulled'")["actor"].iat[0]

    timelines["contributor"] = (
        timelines[["event", "actor"]].groupby("pull_number", group_keys=False).apply(find_contributor)
    )
    return timelines


def fill_last_activity(timelines):
    def find_last_activity(timeline):
        return timeline["time"] == timeline["time"].max()

    timelines["last_activity"] = (
        timelines.query("contributor and event not in ['mentioned', 'subscribed']")[["time"]]
        .groupby("pull_number", group_keys=False)
        .apply(find_last_activity)
    )
    return timelines.fillna({"last_activity": False})


def fill_inactive_days(timelines):
    def find_inactive_days(timeline):
        timeline["inactive_days"] = (DATE - timeline.query("last_activity")["time"].iat[-1]).days
        return timeline[["inactive_days"]]

    timelines["inactive_days"] = (
        timelines[["time", "last_activity"]].groupby("pull_number", group_keys=False).apply(find_inactive_days)
    )
    return timelines.astype({"inactive_days": "uint16"})


def fill_keywords(timelines):
    def find_keywords(timeline):
        if (comments := timeline.query("not contributor and event == 'commented'")["body"]).empty:
            timeline = timeline.assign(**dict.fromkeys(KEYWORDS, False))
        else:
            for keyword in KEYWORDS:
                timeline[keyword] = comments.str.contains(keyword, regex=False).any()
        return timeline[KEYWORDS]

    timelines["body"] = (
        timelines["body"]
        .str.replace(r"(?s)(?:(?<!\\)((?:\\{2})+)(?=`+)|(?<!\\)(`+)(.+?)(?<!`)\2(?!`))", "", regex=True)
        .str.replace(r"(?m)^>.*?$", "", regex=True)
        .str.lower()
    )
    timelines[KEYWORDS] = (
        timelines[["event", "body", "contributor"]].groupby("pull_number", group_keys=False).apply(find_keywords)
    )
    return timelines.drop(columns="body")


def process_chunk(chunk):
    chunk = fill_status(chunk)
    chunk = fill_contributor(chunk)
    chunk = fill_last_activity(chunk)
    chunk = fill_inactive_days(chunk)
    return fill_keywords(chunk)


def export_dataframe(project, dataframe):
    dataframe.to_csv(paths("dataframe", project))


def process_data(project):
    log.info(f"{project}: Processing data")
    with Parallel(n_jobs=-1) as parallel:
        export_dataframe(project, concat(parallel(delayed(process_chunk)(chunk) for chunk in chunk_timelines(project))))


def main():
    projects = []
    for project in preprocessed():
        if cleanup("dataframe", refresh(), project):
            projects.append(project)
        else:
            print(f"Skip processing data for project {project}")
    for project in projects:
        process_data(project)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop processing data")
        exit(1)
