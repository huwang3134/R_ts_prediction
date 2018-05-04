options(error = utils::recover)  # RStudio debug purpose only
# options(error = NULL)
cur_bid_last_k_fold_mape_list <- c(c())
cur_bid_last_k_fold_mean_mape <- c()
cur_bid_last_k_fold_median_mape <- c()
for (cur_bid_idx in seq_along(best_model_list)) {
  cur_bid_str <- bid_str_list[[cur_bid_idx]]
  cur_bid_name <- bid_name_list[[cur_bid_idx]]
  model_heatmap_df <- NULL
  model_fit_time_df <- NULL
  for (cur_pid_str in names(best_model_list[[cur_bid_str]])) {
    cur_cv_info <- best_model_list[[cur_bid_str]][[cur_pid_str]]
    cur_pid_name <-
      cur_cv_info$pid.name
    cur_start_date_str <-
      cur_cv_info$start.date
    cur_end_date_str <-
      cur_cv_info$end.date
    cur_mape_vec_list <-
      cur_cv_info$mape.vec.list
    cur_rmse_vec_list <-
      cur_cv_info$rmse.vec.list
    cur_CV_cnt <-
      length(cur_cv_info$CV.seq)
    
    cur_CV_last_k_fold_mean_mape <-
      sapply(cur_mape_vec_list, function(x) {
        xx <- unlist(x)
        mean(tail(xx, last_k_fold_stat_num), na.rm = TRUE)
      })
    cur_CV_last_k_fold_best_mape_idx <-
      which.min(cur_CV_last_k_fold_mean_mape)
    info(
      logger,
      sprintf(
        "%s '%s' best prediction model is '%s' according to last %d-fold MAPE metric %.4f",
        cur_bid_name,
        cur_pid_name,
        names(cur_CV_last_k_fold_best_mape_idx),
        last_k_fold_stat_num,
        cur_CV_last_k_fold_mean_mape[cur_CV_last_k_fold_best_mape_idx]
      )
    )
    cur_bid_last_k_fold_mape_list[[cur_bid_str]][[cur_pid_str]] <-
      c(cur_bid_last_k_fold_mape_list[[cur_bid_str]][[cur_pid_str]],
        list(cur_CV_last_k_fold_mean_mape[cur_CV_last_k_fold_best_mape_idx]))
    
    
    cur_CV_last_k_fold_mean_rmse <-
      sapply(cur_rmse_vec_list, function(x) {
        xx <- unlist(x)
        mean(tail(xx, last_k_fold_stat_num), na.rm = TRUE)
      })
    cur_CV_last_k_fold_best_rmse_idx <-
      which.min(cur_CV_last_k_fold_mean_rmse)
    info(
      logger,
      sprintf(
        "%s '%s' best prediction model is '%s' according to last %d-fold RMSE metric %.4f",
        cur_bid_name,
        cur_pid_name,
        names(cur_CV_last_k_fold_best_rmse_idx),
        last_k_fold_stat_num,
        cur_CV_last_k_fold_mean_rmse[cur_CV_last_k_fold_best_rmse_idx]
      )
    )
    cur_best_last_k_fold_RMSE <-
      list(name = names(cur_CV_last_k_fold_best_rmse_idx),
           value = cur_CV_last_k_fold_mean_rmse[cur_CV_last_k_fold_best_rmse_idx])
    cur_best_last_k_fold_MAPE <-
      list(name = names(cur_CV_last_k_fold_best_mape_idx),
           value = cur_CV_last_k_fold_mean_mape[cur_CV_last_k_fold_best_mape_idx])
  } # for every pid
  cur_bid_last_k_fold_mape_vec <-
    unlist(cur_bid_last_k_fold_mape_list[[cur_bid_str]], recursive = TRUE)
  cur_bid_last_k_fold_mean_mape[[cur_bid_str]] <-
    mean(cur_bid_last_k_fold_mape_vec,
         na.rm = TRUE)
  cur_bid_last_k_fold_median_mape[[cur_bid_str]] <-
    median(cur_bid_last_k_fold_mape_vec,
           na.rm = TRUE)
}


gen.gam.formula.str <- function(gam_mat, y_freq = 24) {
  gam.mat.colnames <- colnames(gam_mat)
  gam.formula.str <- "y ~"
  has_dayofweek <- FALSE
  has_hourofday <- FALSE
  has_trend <- FALSE
  gam.formula.str
  formula.str.list <- list()
  has_y_col <- FALSE
  for (cur.colname in gam.mat.colnames) {
    if ("y" == cur.colname) {
      has_y_col <- TRUE
      next
    }
    if ("hourofday" == cur.colname) {
      has_hourofday <- TRUE
    }
    if ("dayofweek" == cur.colname) {
      has_dayofweek <- TRUE
    }
    if ("tt" == cur.colname) {
      has_trend <- TRUE
    } else if (class(gam_mat[[cur.colname]]) == "logical") {
      formula.str.list <-
        c(formula.str.list, sprintf("%s", cur.colname))  # dummy indicator variable
    }
  }
  # stopifnot(has_y_col)
  browser()
  if (has_trend) {
    if (nrow(gam_mat) > 7 * y_freq)
      formula.str.list <- c(formula.str.list, "s(tt)")
  } else {
    formula.str.list <-
      c(formula.str.list, "tt")  # significantly better than s(tt) when ts length <= 7 * 24
  }
  if (has_dayofweek && has_hourofday) {
    dayofweek_k <-
      length(unique(gam_mat[["dayofweek"]])) - 1
    hourofday_k <-
      length(unique(gam_mat[["hourofday"]])) - 1
    formula.str.list <- c(
      formula.str.list,
      sprintf(
        "te(hourofday,dayofweek, k = c(%d, %d), bs = c(\"cc\", \"cc\"))",
        hourofday_k,
        dayofweek_k
      )
    )
  } else if (has_dayofweek) {
    dayofweek_k <-
      length(unique(gam_mat[["dayofweek"]])) - 1
    formula.str.list <-
      c(formula.str.list,
        sprintf("s(dayofweek, k = %d, bs = \"cc\")", dayofweek_k))
  } else if (has_hourofday) {
    hourofday_k <-
      length(unique(gam_mat[["hourofday"]])) - 1
    formula.str.list <-
      c(formula.str.list,
        sprintf("s(hourofday, k = %d, bs = \"cc\")", hourofday_k))
  }
  gam.formula.str <-
    paste("y ~", paste(formula.str.list, collapse = " + "), collapse = " ")
  gam.formula.str
}

gen.gam.formula.str(xreg_info_list[["(9)"]][["151068"]][["BoxCox GAM"]][["48"]][["xreg"]][[1]])
