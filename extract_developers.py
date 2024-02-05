from csv import QUOTE_ALL

from pandas import DataFrame, concat, read_csv

from common import cleanup, initialize, logger, paths, preprocessed, refresh

initialize()
log = logger(__file__)


def import_timelines(project):
    return read_csv(
        paths("timelines_preprocessed", project), usecols=["actor", "author.name", "author.email"], quoting=QUOTE_ALL
    ).dropna()


def export_developers(developers):
    DataFrame.from_dict(developers, orient="index").sort_index().rename_axis("actor").to_csv(
        paths("developers"), quoting=QUOTE_ALL
    )


def extract_developers():
    log.info("Extracting developers")
    events = concat([import_timelines(project) for project in preprocessed()]).astype(
        {"actor": "category", "author.name": "category", "author.email": "category"}
    )
    events = events.drop(
        events.query(
            "not `author.email`.str.contains('@', regex=False)"
            " or `author.email`.str.contains('noreply|no-reply', regex=True)"
        ).index
    )
    developers = {}
    for actor in [actor for actor in events["actor"].unique() if actor != "ghost"]:
        actor_events = events.query("actor == @actor")
        developers[actor] = {
            "name": ", ".join(sorted(actor_events["author.name"].unique())),
            "email": ", ".join(sorted(actor_events["author.email"].unique())),
        }
    export_developers(developers)


def main():
    if cleanup("developers", refresh()):
        extract_developers()
    else:
        print("Skip extracting developers")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop extracting developers")
        exit(1)
