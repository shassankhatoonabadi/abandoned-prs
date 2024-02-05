from csv import QUOTE_ALL

from joblib import Parallel, delayed
from numpy import timedelta64
from pandas import DataFrame, Timedelta, read_csv

from common import DATE, KEYWORDS, cleanup, initialize, logger, paths, persist, processed, refresh

initialize()
INACTIVITY = 183


def import_dataframe(project):
    return read_csv(
        paths("dataframe", project),
        index_col=["pull_number", "event_number"],
        dtype={"event": "category", "actor": "category", "author_association": "category", "inactive_days": "uint16"},
        parse_dates=["time", "opened_at", "closed_at", "merged_at"],
        infer_datetime_format=True,
    )


def import_pulls(project):
    return read_csv(
        paths("pulls_preprocessed", project),
        index_col="number",
        usecols=["number", "html_url"],
        dtype={"html_url": "string"},
        quoting=QUOTE_ALL,
    )


def fill_association(dataframe):
    def find_association(events):
        if not (associations := events.query("author_association.notna()")).empty:
            events["author_association"] = associations["author_association"].mode().iat[0]
        else:
            events["author_association"] = "NONE"
        return events[["author_association"]]

    dataframe["author_association"] = (
        dataframe[["actor", "author_association"]].groupby("actor", group_keys=False).apply(find_association)
    )
    return dataframe.astype({"author_association": "category"})


def fill_core(dataframe):
    dataframe["core"] = dataframe["author_association"].isin(["OWNER", "MEMBER", "COLLABORATOR"])
    return dataframe.drop(columns="author_association")


def fill_abandoned(dataframe):
    dataframe["abandoned"] = (
        ~dataframe["merged"] & (dataframe["inactive_days"] >= INACTIVITY) & dataframe[KEYWORDS].any(axis="columns")
    )
    return dataframe.drop(columns="inactive_days")


def select_pulls(dataframe, columns=None):
    if columns is not None:
        if not isinstance(columns, list):
            columns = [columns]
        dataframe = dataframe.query(" and ".join([f"`{column}`" for column in columns]))
    return dataframe.index.unique("pull_number")


def export_dataset(project, dataset):
    dataset.to_csv(paths("dataset", project))


def export_sample(project, sample, pulls):
    pulls.sample(frac=1, random_state=1).query("number in @sample").to_csv(paths("sample", project))


def postprocess_data(project):
    log = logger(__file__, modules={"sqlitedict": "WARNING"})
    log.info(f"{project}: Postprocessing data")
    dataframe = import_dataframe(project)
    metadata = persist(paths("metadata", project))
    dataframe = fill_association(dataframe)
    dataframe = fill_core(dataframe)
    dataframe = fill_abandoned(dataframe)
    pulled_dataframe = dataframe.query("event == 'pulled'")
    dataset = dataframe[
        ~dataframe.index.get_level_values("pull_number").isin(
            pulled_dataframe[
                (pulled_dataframe["opened_at"] >= DATE - Timedelta(days=INACTIVITY))
                | pulled_dataframe["core"]
                | (pulled_dataframe["actor"] == "ghost")
            ].index.get_level_values("pull_number")
        )
    ]
    pulled_dataset = dataset.query("event == 'pulled'")
    sample = select_pulls(pulled_dataset, "abandoned")
    statistics = {
        "project": project,
        "language": metadata["language"],
        "stars": metadata["watchers"],
        "months": (pulled_dataframe["time"].max() - pulled_dataframe["time"].min()) // timedelta64(1, "M"),
        "months-": (pulled_dataset["time"].max() - pulled_dataset["time"].min()) // timedelta64(1, "M"),
        "cores": pulled_dataframe.query("core")["actor"].nunique(),
        "cores-": pulled_dataset.query("core")["actor"].nunique(),
        "contributors": pulled_dataframe.query("not core")["actor"].nunique(),
        "contributors-": pulled_dataset.query("not core")["actor"].nunique(),
        "pulls": len(pulled_dataframe),
        "pulls-": len(pulled_dataset),
        "open": len(select_pulls(pulled_dataframe, "open")),
        "open-": len(select_pulls(pulled_dataset, "open")),
        "closed": len(select_pulls(pulled_dataframe, "closed")),
        "closed-": len(select_pulls(pulled_dataset, "closed")),
        "merged": len(select_pulls(pulled_dataframe, "merged")),
        "merged-": len(select_pulls(pulled_dataset, "merged")),
        "abandoned": len(select_pulls(pulled_dataframe, "abandoned")),
        "abandoned-": len(sample),
    }
    for keyword in KEYWORDS:
        statistics[keyword] = len(select_pulls(pulled_dataframe, keyword))
        statistics[f"{keyword}-"] = len(select_pulls(pulled_dataset, keyword))
    export_dataset(project, dataset.drop(columns=KEYWORDS))
    export_sample(project, sample, import_pulls(project))
    return statistics


def export_statistics(statistics):
    file = paths("statistics")
    DataFrame(statistics).to_csv(file, header=False if file.exists() else True, index=False, mode="a")


def main():
    fresh = refresh()
    if not cleanup("statistics", fresh):
        print("Skip refreshing statistics")
    projects = []
    for project in processed():
        if cleanup(["dataset", "sample"], fresh, project):
            projects.append(project)
        else:
            print(f"Skip postprocessing data for project {project}")
    with Parallel(n_jobs=-1) as parallel:
        export_statistics(parallel(delayed(postprocess_data)(project) for project in projects))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop postprocessing data")
        exit(1)
