centrality_ggrepel <- function(plot,
                               data,
                               x,
                               y,
                               centrality.path = FALSE,
                               centrality.path.args = list(
                                   color = "red",
                                   size = 1,
                                   alpha = 0.5
                               ),
                               centrality.point.args = list(
                                   size = 5,
                                   color = "darkred"
                               ),
                               centrality.label.args = list(
                                   size = 3,
                                   nudge_x = 0.4,
                                   segment.linetype = 4,
                                   min.segment.length = 0
                               ),
                               ...) {
    # creating the dataframe
    centrality_df <- suppressWarnings(centrality_description(data, {{ x }}, {{ y }}, ...))
    maximum <- max(centrality_df[y])
    centrality_df %<>% mutate(expression = glue("M=='{format_value(get(y), ifelse(maximum > 0 & maximum < 1, 3, 0))}'"))

    # if there should be lines connecting mean values across groups
    if (isTRUE(centrality.path)) {
        plot <- plot +
            exec(
                geom_path,
                data = centrality_df,
                mapping = aes(x = {{ x }}, y = {{ y }}, group = 1),
                inherit.aes = FALSE,
                !!!centrality.path.args
            )
    }

    # highlight the mean of each group
    plot +
        exec(
            geom_point,
            mapping = aes({{ x }}, {{ y }}),
            data = centrality_df,
            inherit.aes = FALSE,
            !!!centrality.point.args
        ) + # attach the labels with means to the plot
        exec(
            ggrepel::geom_label_repel,
            data = centrality_df,
            mapping = aes(x = {{ x }}, y = {{ y }}, label = expression),
            inherit.aes = FALSE,
            parse = TRUE,
            !!!centrality.label.args
        ) + # adding sample size labels to the x axes
        scale_x_discrete(labels = c(unique(centrality_df$n_label)))
}

environment(centrality_ggrepel) <- asNamespace("ggstatsplot")
assignInNamespace("centrality_ggrepel", centrality_ggrepel, "ggstatsplot")
