from csv import QUOTE_ALL

from joblib import Parallel, delayed
from numpy import timedelta64
from pandas import DataFrame, Timestamp, concat, notna, read_csv

from common import DATE, cleanup, initialize, logger, paths, persist, postprocessed, refresh

initialize()


def import_dataset(project):
    return read_csv(
        paths("dataset", project),
        index_col=["pull_number", "event_number"],
        dtype={"event": "category", "actor": "category"},
        parse_dates=["time", "opened_at", "closed_at", "merged_at"],
        infer_datetime_format=True,
    )


def import_pulls(project):
    return read_csv(
        paths("pulls_preprocessed", project),
        index_col="number",
        usecols=["number", "title", "body"],
        dtype={"title": "string", "body": "string"},
        quoting=QUOTE_ALL,
    ).fillna("")


def export_features(project, features):
    DataFrame(features).to_csv(paths("features", project), index=False)


def measure_features(project):
    log = logger(__file__, modules={"sqlitedict": "WARNING"})
    log.info(f"{project}: Measuring features")
    dataset = import_dataset(project)
    pulls = import_pulls(project)
    files = persist(paths("files", project))
    metadata = persist(paths("metadata", project))
    features = []
    created_at = Timestamp(metadata["created_at"]).tz_convert(tz=None)
    for pull_number in dataset.index.unique("pull_number"):
        timeline = dataset.query("pull_number == @pull_number")
        pulled = timeline.query("event == 'pulled'")
        past_pulled = dataset.query("event == 'pulled' and pull_number < @pull_number")
        contributor_pulled = past_pulled.query("actor == @pulled['actor'].iat[0]")
        contributor_pulls = len(contributor_pulled)
        responses = timeline.query(
            "event in ['commented', 'reviewed', 'line-commented', 'commit-commented'] and time > opened_at"
        )
        participant_responses = responses.query("not contributor")
        changed_lines = 0
        changed_files = 0
        for file in files[pull_number].values():
            changed_lines += file["changes"]
            changed_files += 1
        opened_at = pulled["opened_at"].iat[0]
        closed_at = pulled["closed_at"].iat[0]
        merged_at = pulled["merged_at"].iat[0]
        if notna(merged_at):
            lifetime = merged_at - opened_at
        elif notna(closed_at):
            lifetime = closed_at - opened_at
        else:
            lifetime = DATE - opened_at
        features.append(
            {
                # Identifiers
                "project": project,
                "pull_number": pull_number,
                "open": pulled["open"].iat[0],
                "closed": pulled["closed"].iat[0],
                "merged": pulled["merged"].iat[0],
                "abandoned": pulled["abandoned"].iat[0],
                # PR Features
                "pr_description": len((pulls.loc[pull_number, "title"] + " " + pulls.loc[pull_number, "body"]).split()),
                "pr_commits": len(timeline.query("event == 'committed'")),
                "pr_changed_lines": changed_lines,
                "pr_changed_files": changed_files,
                "pr_lifetime": lifetime // timedelta64(1, "D"),
                # Contributor Features
                "contributor_pulls": contributor_pulls,
                "contributor_contribution_period": (
                    (opened_at - contributor_pulled["opened_at"].min()) // timedelta64(1, "M")
                    if not contributor_pulled.empty
                    else 0
                ),
                "contributor_acceptance_rate": (
                    len(contributor_pulled.query("merged_at < @opened_at")) / contributor_pulls
                    if contributor_pulls
                    else 0
                ),
                "contributor_abandonment_rate": (
                    len(contributor_pulled.query("abandoned")) / contributor_pulls if contributor_pulls else 0
                ),
                # Review Process Features
                "review_participants": participant_responses["actor"].nunique(),
                "review_participant_responses": len(participant_responses),
                "review_contributor_responses": len(responses.query("contributor")),
                "review_response_latency": (
                    (participant_responses["time"].min() - opened_at if not participant_responses.empty else lifetime)
                    // timedelta64(1, "D")
                ),
                "review_responses_interval": (
                    (
                        concat([pulled, participant_responses])["time"].diff().mean()
                        if not participant_responses.empty
                        else lifetime
                    )
                    // timedelta64(1, "D")
                ),
                # Project Features
                "project_age": (opened_at - created_at) // timedelta64(1, "M"),
                "project_pulls": len(past_pulled),
                "project_contributors": past_pulled["actor"].nunique(),
                "project_unresolved_pulls": len(
                    past_pulled.query("merged_at >= @opened_at or closed_at >= @opened_at or open")
                ),
            }
        )
    export_features(project, features)


def main():
    projects = []
    for project in postprocessed():
        if cleanup("features", refresh(), project):
            projects.append(project)
        else:
            print(f"Skip measuring features for project {project}")
    with Parallel(n_jobs=-1) as parallel:
        parallel(delayed(measure_features)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop measuring features")
        exit(1)
