from numpy import timedelta64
from pandas import concat, read_csv

from common import cleanup, initialize, logger, paths, postprocessed, refresh

initialize()
log = logger(__file__)


def import_dataset(project):
    return read_csv(
        paths("dataset", project),
        usecols=["event", "opened_at", "merged_at", "merged", "core"],
        dtype={"event": "category"},
        parse_dates=["opened_at", "merged_at"],
        infer_datetime_format=True,
    )


def export_inactivity(inactivity):
    inactivity.to_csv(paths("inactivity"), index=False)


def analyze_inactivity():
    log.info("Analyzing inactivity")
    data = concat(
        [
            import_dataset(project).query("event == 'pulled' and merged and not core")[["opened_at", "merged_at"]]
            for project in postprocessed()
        ]
    )
    data["lifetime"] = (data["merged_at"] - data["opened_at"]) // timedelta64(1, "M")
    data = data.query("lifetime >= 0").value_counts("lifetime", sort=False).to_frame("frequency").reset_index()
    data["pdf"] = data["frequency"] / sum(data["frequency"])
    data["cdf"] = data["pdf"].cumsum()
    export_inactivity(data)


def main():
    if cleanup("inactivity", refresh()):
        analyze_inactivity()
    else:
        print("Skip analyzing inactivity")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop analyzing inactivity")
        exit(1)
