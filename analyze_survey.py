from csv import QUOTE_ALL

from pandas import concat, read_csv

from common import cleanup, initialize, logger, paths, postprocessed, refresh

initialize()
log = logger(__file__)


def import_responses(project):
    return read_csv(paths("responses", project), quoting=QUOTE_ALL).assign(project=project.split("/")[1])


def export_survey():
    log.info("Exporting survey responses")
    concat([import_responses(project) for project in postprocessed()]).set_index("project").sort_index().to_excel(
        paths("survey"), freeze_panes=(1, 1)
    )


def main():
    if cleanup("survey", refresh()):
        export_survey()
    else:
        print("Skip analyzing survey")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop analyzing survey")
        exit(1)
