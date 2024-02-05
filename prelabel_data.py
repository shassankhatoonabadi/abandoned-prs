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


def export_prelabeling():
    log.info("Exporting prelabeling dataset")
    concat([import_sample(project).head(10) for project in postprocessed()]).to_csv(paths("prelabeling"))


def main():
    if cleanup("prelabeling", refresh()):
        export_prelabeling()
    else:
        print("Skip prelabeling dataset")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop prelabeling dataset")
        exit(1)
