import::from(cowplot, as_grob, save_plot)
import::from(Dict, Dict)
import::from(doFuture, registerDoFuture)
import::from(dplyr, bind_rows, filter, if_all, select)
import::from(forcats, as_factor, fct_recode, fct_relevel)
import::from(future, multicore, plan)
import::from(
    ggplot2, element_blank, element_text, geom_point, labs, margin, scale_x_continuous, scale_y_continuous, theme
)
import::from(ggpubr, annotate_figure, ggarrange, grids, text_grob, theme_pubr)
import::from(ggstatsplot, grouped_ggbetweenstats)
import::from(glue, glue)
import::from(Hmisc, redun, varclus)
import::from(iml, FeatureEffect, FeatureImp, Predictor)
import::from(magrittr, "%>%", "%<>%")
import::from(mlr, auc, getPredictionProbabilities, makeClassifTask, makeMeasure, measureAUC, repcv)
import::from(mlr3measures, prauc)
import::from(readr, read_csv, write_csv)
import::from(scales, label_number, label_number_si)
import::from(stringr, str_replace)
import::from(tibble, rownames_to_column, tibble)
import::from(tuneRanger, tuneRanger)

source("statsExpressions.R")
source("ggstatsplot.R")

registerDoFuture()
plan(multicore)

setwd("data/")
sink(file = "analytics.log", split = TRUE)

projects <- Dict$new(
    "ansible/ansible" = "Ansible",
    "definitelytyped/definitelytyped" = "DefinitelyTyped",
    "elastic/elasticsearch" = "Elasticsearch",
    "homebrew/homebrew-cask" = "Homebrew Cask",
    "elastic/kibana" = "Kibana",
    "kubernetes/kubernetes" = "Kubernetes",
    "homebrew/legacy-homebrew" = "Legacy Homebrew",
    "odoo/odoo" = "Odoo",
    "rust-lang/rust" = "Rust",
    "apple/swift" = "Swift"
)

projects_data <- tibble()

for (project in projects$keys) {
    pattern <- str_replace(project, "/", "_")
    data <- read_csv(glue("{pattern}/{pattern}_features.csv"), show_col_types = FALSE)
    data$project %<>% str_replace(project, projects[project])
    projects_data %<>% bind_rows(data)
}

projects_data %<>%
    select(-c(pull_number, open, closed, merged)) %>%
    filter(if_all(where(is.numeric), ~ . >= 0))

projects_data$abandoned %<>%
    as_factor() %>%
    fct_recode(Abandoned = "TRUE", Nonabandoned = "FALSE") %>%
    fct_relevel("Abandoned")

for (feature in names(select(projects_data, where(is.numeric)))) {
    print(glue("Creating descriptive plots for feature {feature}"))

    set.seed(1)
    grouped_ggbetweenstats(
        projects_data,
        x = abandoned,
        y = !!feature,
        grouping.var = project,
        type = "nonparametric",
        k = 3,
        centrality.point.args = list(size = 0),
        centrality.label.args = list(size = 2.5, nudge_x = 0.4, min.segment.length = 0),
        point.args = list(size = 0),
        violin.args = list(width = 0.5, alpha = 0.1),
        plotgrid.args = list(nrow = 2),
        ggplot.component = list(
            scale_y_continuous(labels = label_number_si()),
            theme(
                axis.title = element_blank(),
                plot.margin = margin(2.5, 5, 2.5, 5),
                plot.subtitle = element_text(hjust = 0.5),
                plot.title = element_text(size = 10, hjust = 0.5)
            )
        )
    ) %>%
        as_grob() %>%
        annotate_figure(left = text_grob(feature, rot = 90, face = "bold", size = 11)) %>%
        save_plot(glue("{feature}_stats.png"), plot = ., base_height = 4, base_width = 13)
}

projects_data %<>% select(-c(pr_lifetime, review_responses_interval, pr_changed_files))

pdf("correlations.pdf", width = 6.75, height = 4.5)
par(mar = c(0.25, 4.25, 0, 0))
plot(varclus(abandoned ~ ., data = select(projects_data, -project), trans = "abs"))
abline(h = 0.4, col = "red", lty = "dotted", lwd = 2)
dev.off()

projects_data %<>% select(-c(project_pulls, project_contributors, contributor_contribution_period, review_participants))
print(redun(~., select(projects_data, -c(abandoned, project)), r2 = 0.6, nk = 0))

projects <- projects$values
project_data <- list()

for (project in projects) {
    project_data[[project]] <-
        projects_data %>%
        filter(project == !!project) %>%
        select(-project)
}

aucpr <- makeMeasure(
    id = "aucpr", minimize = FALSE, best = 1, worst = 0,
    properties = c("classif", "req.pred", "req.truth", "req.prob"),
    fun = function(task, model, pred, feats, extra.args) {
        prauc(pred$data$truth, getPredictionProbabilities(pred), pred$task.desc$positive)
    }
)

models <- list()
performances <- data.frame()

for (project in projects) {
    print(glue("Building and evaluating model for project {project}"))
    task <- makeClassifTask(data = project_data[[project]], target = "abandoned")

    set.seed(1)
    models[[project]] <- tuneRanger(task, measure = list(auc))$model

    set.seed(1)
    prediction <- repcv(models[[project]]$learner, task, stratify = TRUE, measures = list(auc, aucpr))

    class <- task$task.desc$class.distribution
    performances[project, c("auc-roc", "auc-pr")] <- prediction$aggr
    performances[project, "ratio"] <- class["Abandoned"] / (class["Abandoned"] + class["Nonabandoned"])
}

write_csv(rownames_to_column(performances, "project"), "performance.csv")
save.image()

importances <- list()
auc_error <- function(actual, predicted) 1 - measureAUC(predicted, actual, positive = "Abandoned")

for (project in projects) {
    print(glue("Analyzing feature importance for project {project}"))

    set.seed(1)
    importances[[project]] <-
        models[[project]] %>%
        Predictor$new(project_data[[project]], class = "Abandoned") %>%
        FeatureImp$new(loss = auc_error, n.repetitions = 100)
}

features <- names(select(projects_data, where(is.numeric)))
importance <- data.frame()

for (feature in features) {
    for (project in projects) {
        importance[feature, project] <- filter(importances[[project]]$results, feature == !!feature)$importance
    }
}

write_csv(rownames_to_column(importance, "feature"), "importance.csv")
save.image()

ale_plots <- list()

for (feature in features) {
    print(glue("Creating ALE plots for feature {feature}"))

    for (project in projects) {
        data <- project_data[[project]]

        if (max(data[[feature]]) > 1) {
            data %<>% filter(get(feature) <= quantile(data[[feature]], 0.99))
        }

        ale_plots[[feature]][[project]] <-
            models[[project]] %>%
            Predictor$new(data, class = "Abandoned") %>%
            FeatureEffect$new(feature, grid.size = 10) %>%
            plot() +
            geom_point(size = 0.75) +
            labs(subtitle = project) +
            scale_x_continuous(labels = label_number_si(drop0trailing = TRUE)) +
            scale_y_continuous(labels = label_number(scale = 100)) +
            theme_pubr() +
            grids(linetype = "solid") +
            theme(
                axis.title = element_blank(),
                plot.background = element_blank(),
                plot.margin = margin(2.5, 7.5, 0, 0),
                plot.subtitle = element_text(face = "bold", hjust = 0.5)
            )
    }

    ggarrange(plotlist = ale_plots[[feature]], ncol = 5, nrow = 2, align = "hv") %>%
        annotate_figure(
            bottom = text_grob(feature, face = "bold"),
            left = text_grob("ALE of Abandonment (100X)", rot = 90, face = "bold")
        ) %>%
        save_plot(glue("{feature}_ale.png"), plot = ., base_height = 4.5, base_width = 14.5)
}

save.image()
