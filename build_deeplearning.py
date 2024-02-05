from joblib import Parallel, delayed
from keras import Sequential
from keras.layers import Dense
from pandas import DataFrame, read_csv
from scikeras.wrappers import KerasClassifier
from sklearn.model_selection import RepeatedStratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.utils import set_random_seed

from common import cleanup, initialize, logger, measured, paths, refresh

initialize()


def import_features(project):
    file = paths("features", project)
    return read_csv(
        file,
        usecols=[
            column
            for column in read_csv(file, nrows=0)
            if column
            not in [
                "project",
                "pull_number",
                "open",
                "closed",
                "merged",
                "pr_changed_files",
                "pr_lifetime",
                "contributor_contribution_period",
                "review_participants",
                "review_responses_interval",
                "project_pulls",
                "project_contributors",
            ]
        ],
    )


def create_model():
    model = Sequential(
        [
            Dense(11, activation="relu", input_dim=11),
            Dense(6, activation="relu"),
            Dense(1, activation="sigmoid"),
        ]
    )
    model.compile(optimizer="Adam", loss="binary_crossentropy")
    return model


def build_deeplearning(project):
    log = logger(__file__)
    log.info(f"{project}: Building deep learning model")
    set_random_seed(1)
    features = import_features(project).values
    X = features[:, 1:].astype(float)
    y = features[:, 0].astype(float)
    X = StandardScaler().fit(X).transform(X)
    results = cross_val_score(
        KerasClassifier(create_model), X, y, scoring="roc_auc", cv=RepeatedStratifiedKFold(n_splits=10)
    )
    return {"project": project, "auc": results.mean()}


def export_scores(scores):
    DataFrame(scores).to_csv(paths("deeplearning"), index=False)


def main():
    if cleanup("deeplearning", refresh()):
        with Parallel(n_jobs=-1) as parallel:
            export_scores(parallel(delayed(build_deeplearning)(project) for project in measured()))
    else:
        print("Skip building deep learning models")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop building deep learning models")
        exit(1)
