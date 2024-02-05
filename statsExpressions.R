# effect sizes from (Hess and Kromrey, 2004)
mag.levels <- c(0.147, 0.33, 0.474)
magnitude <- c("Negligible", "Small", "Medium", "Large")

add_expression_col <- function(data,
                               paired = FALSE,
                               statistic.text = NULL,
                               effsize.text = NULL,
                               top.text = NULL,
                               prior.type = NULL,
                               n = NULL,
                               n.text = ifelse(
                                   paired,
                                   list(quote(italic("n")["pairs"])),
                                   list(quote(italic("n")["obs"]))
                               ),
                               conf.method = "HDI",
                               k = 2L,
                               k.df = 0L,
                               k.df.error = k.df,
                               ...) {

    # some cleanup before we begin
    data %<>%
        rename_all(.funs = recode, "bayes.factor" = "bf10") %>%
        mutate(
            effectsize = ifelse("effectsize" %in% names(.), effectsize, method),
            n.obs = n
        )

    # is this Bayesian test?
    bayesian <- ifelse("bf10" %in% names(data), TRUE, FALSE)

    # special case for Bayesian analysis
    if (bayesian && grepl("contingency", data$method[[1]])) data %<>% mutate(effectsize = "Cramers_v")

    # convert needed columns to character type
    df_expr <- .data_to_char(data, k, k.df, k.df.error)

    # adding a few other columns
    df_expr %<>% mutate(
        statistic.text     = statistic.text %||% stat_text_switch(method),
        es.text            = effsize.text %||% estimate_type_switch(effectsize),
        prior.distribution = prior_switch(method),
        conf.method        = toupper(conf.method),
        n.obs              = .prettyNum(n)
    )

    # Bayesian analysis ------------------------------

    if (bayesian) {
        if (is.null(top.text)) {
            df_expr %<>% mutate(expression = glue("list(
            log[e]*(BF['01'])=='{format_value(-log(bf10), k)}',
            {es.text}^'posterior'=='{estimate}',
            CI['{conf.level}']^{conf.method}~'['*'{conf.low}', '{conf.high}'*']',
            {prior.distribution}=='{prior.scale}')"))
        } else {
            df_expr %<>% mutate(expression = glue("list(
            atop('{top.text}',
            list(log[e]*(BF['01'])=='{format_value(-log(bf10), k)}',
            {es.text}^'posterior'=='{estimate}',
            CI['{conf.level}']^{conf.method}~'['*'{conf.low}', '{conf.high}'*']',
            {prior.distribution}=='{prior.scale}')))"))
        }
    }

    # how many parameters?
    no.parameters <- sum("df.error" %in% names(data) + "df" %in% names(data))

    # 0 degrees of freedom --------------------

    if (!bayesian && no.parameters == 0L) {
        df_expr %<>%
            mutate(
                p.value = insight::format_p(p.value, stars = TRUE, name = "", digits = "apa"),
                magnitude = magnitude[findInterval(abs(as.numeric(estimate)), mag.levels) + 1]
            ) %>%
            mutate(expression = glue("list(
            italic(p)*'{p.value}',
            italic(d)=='{estimate}'~('{magnitude}'))"))
    }

    # 1 degree of freedom --------------------

    if (!bayesian && no.parameters == 1L) {
        # for chi-squared statistic
        if ("df" %in% names(df_expr)) df_expr %<>% mutate(df.error = df)

        df_expr %<>% mutate(expression = glue("list(
            {statistic.text}*'('*{df.error}*')'=='{statistic}', italic(p)=='{p.value}',
            {es.text}=='{estimate}', CI['{conf.level}']~'['*'{conf.low}', '{conf.high}'*']',
            {n.text}=='{n.obs}')"))
    }

    # 2 degrees of freedom -----------------

    if (!bayesian && no.parameters == 2L) {
        df_expr %<>% mutate(expression = glue("list(
            {statistic.text}({df}, {df.error})=='{statistic}', italic(p)=='{p.value}',
            {es.text}=='{estimate}', CI['{conf.level}']~'['*'{conf.low}', '{conf.high}'*']',
            {n.text}=='{n.obs}')"))
    }

    # return dataframe with some polish and formatted expression
    as_tibble(data) %>%
        relocate(matches("^effectsize$"), .before = matches("^estimate$")) %>%
        mutate(expression = list(parse(text = df_expr$expression[[1]])))
}

environment(add_expression_col) <- asNamespace("statsExpressions")
assignInNamespace("add_expression_col", add_expression_col, "statsExpressions")
