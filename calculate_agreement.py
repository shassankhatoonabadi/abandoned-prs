from pandas import DataFrame, concat, notna, read_excel
from sklearn.metrics import cohen_kappa_score

from common import cleanup, initialize, logger, paths, refresh

initialize()
log = logger(__file__)


def import_labels():
    return read_excel(
        paths("labels"),
        sheet_name=["Hassan", "Diego", "Rabe"],
        index_col=[0, 1],
        usecols=["Project", "ID", "First Reason", "Second Reason"],
    )


def fill_common(row):
    first_hassan = row["First Reason Hassan"]
    second_hassan = row["Second Reason Hassan"]
    first_diego = row["First Reason Diego"]
    second_diego = row["Second Reason Diego"]
    if first_hassan in [first_diego, second_diego]:
        row["first"] = row["second"] = first_hassan
    elif notna(second_hassan) and second_hassan in [first_diego, second_diego]:
        row["first"] = row["second"] = second_hassan
    elif notna(first_diego):
        row["first"] = first_hassan
        row["second"] = first_diego
    elif notna(second_diego):
        row["first"] = first_hassan
        row["second"] = second_diego
    return row[["first", "second"]]


def export_score(score):
    DataFrame({"score": [score]}).to_csv(paths("agreement"), index=False)


def calculate_agreement():
    log.info("Calculating agreement")
    hassan, diego, rabe = import_labels().values()
    labels = hassan.join(concat([rabe, diego]), lsuffix=" Hassan", rsuffix=" Diego")
    labels[["first", "second"]] = labels.apply(fill_common, axis="columns")
    export_score(cohen_kappa_score(labels["first"], labels["second"]))


def main():
    if cleanup("agreement", refresh()):
        calculate_agreement()
    else:
        print("Skip calculating agreement")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop calculating agreement")
        exit(1)
