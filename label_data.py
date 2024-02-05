from pandas import concat, read_csv

from common import cleanup, initialize, logger, paths, postprocessed, refresh

initialize()
log = logger(__file__)


def import_sample(project):
    return (
        read_csv(paths("sample", project), dtype={"html_url": "string"})
        .assign(project=project)
        .set_index(["project", "number"])
    )


def export_labeling():
    log.info("Exporting labeling dataset")
    concat([import_sample(project) for project in postprocessed()]).sample(frac=1, random_state=1).to_csv(
        paths("labeling")
    )


def main():
    if cleanup("labeling", refresh()):
        export_labeling()
    else:
        print("Skip labeling dataset")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop labeling dataset")
        exit(1)
