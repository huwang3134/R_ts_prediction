options(error = utils::recover)  # RStudio debug purpose only
# options(error = NULL)

# clean up mess from previous aborted run
tryCatch({
  stopCluster(cl)  # timeout or have pending rows
}, error = function (e) {
  error(logger, as.character(e))
})

tryCatch({
  mysql_dbDisconnectAll(mysql.drv)  # timeout or have pending rows
}, error = function (e) {
  error(logger, as.character(e))
})

tryCatch({
  postgresql_dbDisconnectAll(hourly.vv.pg.drv)  # timeout or have pending rows
}, error = function (e) {
  warn(logger, as.character(e))
})
tryCatch({
  postgresql_dbDisconnectAll(daily.vv.pg.drv)
}, error = function (e) {
  warn(logger, as.character(e))
})
rm(list = ls())

program.start.time <- Sys.time()

verbose <- FALSE
using_tsoutliers <- FALSE
using_update_hour <-
  TRUE # no pid FUTURE update hour in db yet, using this as a test of how good this predictor might be
top_pid_cnt <- 30
# top_pid_cnt <- 7

max_train_size <- 24 * 14
last_k_fold_stat_num <- 7
max_lag_allow <- 24

test_ts_list <- c()
test_xts_list <- c()
best_model_list <- c()
model_fit_list <- c()  # this one is gigantic for reading from and writing to file
model_fit_time_list <- c()
xreg_info_list <- c()
update_time_single_list <- c()
# max_sample_size <- 120
# test_mean <- 100000
# test_sd <- test_mean * 0.3
y_freq <- 24
hold_out_h <- y_freq

require("log4r")
logger <- create.logger(logfile = "", level = "INFO")


gg_color_hue <- function(n) {
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[1:n]
}


comb <-
  function(x, ...) {
    # combiner to retrieve %dopar% result into a list
    lapply(seq_along(x),
           function(i)
             c(x[[i]], lapply(list(...), function(y)
               y[[i]])))
  }


# trim column by step-wise VIF of "usdm" package
trim.col.vif <- function(xreg, fxreg, th = 10) {
  require("usdm")
  v2 <- vifstep(xreg, th = th)
  sel_colnames <- as.character(v2@results$Variables)
  excluded_colnames <- as.character(v2@excluded)
  cur_filtered_xreg <- xreg[, sel_colnames, drop = FALSE]
  if (length(excluded_colnames) > 0) {
    info(logger,
         sprintf(
           "\tpredictors '%s' is excluded by th value %f",
           paste(excluded_colnames, collapse = ","),
           th
         ))
  }
  xreg <- cur_filtered_xreg
  stopifnot(all(sel_colnames %in% colnames(fxreg)))
  fxreg <- fxreg[, sel_colnames, drop = FALSE]
  list(xreg = xreg,
       fxreg = fxreg,
       excluded_colnames = excluded_colnames)
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
  if (has_trend) {
    if (nrow(gam_mat) > 7 * y_freq)
      # formula.str.list <- c(formula.str.list, "s(tt)")
      formula.str.list <- c(formula.str.list, "tt")
    else {
      formula.str.list <-
        c(formula.str.list, "tt")  # significantly better than s(tt) when ts length <= 7 * 24
    }
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


sel.fourier.K <- function(x) {
  # https://robjhyndman.com/hyndsight/forecasting-weekly-data/#regression-with-arima-errors
  bestfit <- list(AICc = Inf, K = NA)
  if (any(class(x) == "msts")) {
    period <- attr(x, "msts")
  } else {
    period <- frequency(x)
  }
  max.K <- sapply(floor(period / 2), function(x)
    min(25, x))
  expand_list <- NULL
  for (i in seq_along(max.K)) {
    expand_list <- c(expand_list, list(1:max.K[i]))
  }
  K.combo.df <- expand.grid(expand_list)
  K.combo.list <- split(K.combo.df, seq(nrow(K.combo.df)))
  for (i in seq_along(length(K.combo.list)))
  {
    cur.K <- as.numeric(K.combo.list[[i]])
    cur_fourier <- fourier(x, K = cur.K)
    fit <-
      tslm(x ~ cur_fourier)  # auto.arima is too slow for this
    cur_CV <- forecast::CV(fit)
    if (cur_CV["AICc"] < bestfit$AICc) {
      bestfit <- list(AICc = cur_CV["AICc"], K = cur.K)
    }
    else
      break
  }
  bestfit
}


collect.cv.res <-
  function(cur_train_h_seq,
           cv_res_list,
           model_fit_list,
           model_fit_time_list,
           forecast_list,
           h = 24) {
    for (train_h_i in seq_along(cur_train_h_seq)) {
      train_h <- cur_train_h_seq[[train_h_i]]
      cur_cv_res <- cv_res_list[[train_h_i]]
      cur.model.fit <- cur_cv_res$cur.model.fit
      cur.fit.time.taken <- cur_cv_res$fit.time.taken
      cur_train_h_str <-
        as.character(train_h)
      # no elegent way to pass by reference
      model_fit_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <<-
        c(model_fit_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
          list(cur.model.fit))
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <<-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
          list(as.numeric(cur.fit.time.taken, units = "secs")))
      cur.pred.values <- cur_cv_res$cur.pred.values
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <<-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
          list(cur.pred.values[1:hold_out_h]))
    }
  }


tsoutliers.xreg <- function(cur_train_y_ts, h) {
  cur.train.size <- length(cur_train_y_ts)
  xreg <- NA
  fxreg <- NA
  require("tsoutliers")
  tryCatch({
    cur_tsoutliers <-
      tso(cur_train_y_ts, types = c("AO", "LS", "TC")) # Error in rowSums(xregg) : 'x' must be an array of at least two dimensions
    if (0 != nrow(cur_tsoutliers$outliers)) {
      xreg <- cur_tsoutliers$fit$xreg
      outliers_newxreg <-
        outliers.effects(cur_tsoutliers$outliers,
                         cur.train.size + h) # Error in oeff[, indao] <- AOeffect(n, mo[indao, "ind"], mo[indao, "coefhat"]) :
      newxreg_colnames <- colnames(outliers_newxreg)
      outliers_newxreg <-
        as.matrix(outliers_newxreg[-seq_along(cur_train_y_ts), ])
      colnames(outliers_newxreg) <- newxreg_colnames
      fxreg <- outliers_newxreg
    }
  }, error = function(e) {
    log4r::error(logger,
                 sprintf("tsoutliers::tso:\t'%s'", as.character(e)))
    print(as.character(e))
    print(sys.calls())
  })
  list(cur.train.size = cur.train.size,
       xreg = xreg,
       fxreg = fxreg)
}


last.seas <-
  function(object, h) {
    # https://github.com/robjhyndman/forecast/blob/991c39760d64dc43c2438f6c1c63597fbb754c91/R/mstl.R#L315
    lastseas <- NULL
    if ("stl" %in% class(object)) {
      m <- frequency(object$time.series)
      n <- NROW(object$time.series)
      lastseas <-
        rep(seasonal(object)[n - (m:1) + 1], trunc(1 + (h - 1) / m))[1:h]
      lastseas
    }
  }


is.rank.deficient <- function(xreg) {
  test_res <- FALSE
  xregg <- as.matrix(xreg)
  sv <- svd(na.omit(cbind(rep(1, NROW(
    xregg
  )), xregg)))$d
  if (min(sv) / sum(sv) < .Machine$double.eps)
    test_res <- TRUE
  test_res
}


model.df <- function(object) {
  # shameless ripoff from auto.arima source code
  df <- NA
  if (is.element("Arima", class(object))) {
    df <- length(object$coef)
    
  } else if (is.element("bats", class(object))) {
    df <-
      length(object$parameters$vect) + NROW(object$seed.states)
  } else if (is.element("ets", class(object)))
    df <- length(object$par)
  else if (is.element("lm", class(object)))
    df <- length(coef(object))
  df
}


mysql_dbDisconnectAll <- function(drv) {
  ile <- length(dbListConnections(drv))
  lapply(dbListConnections(drv), function(conn) {
    # dbClearResult(dbListResults(conn)[[1]])
    dbDisconnect(conn)
  })
  cat(sprintf("%s connection(s) closed.\n", ile))
}


postgresql_dbDisconnectAll <- function(drv) {
  for (conn in dbListConnections(drv)) {
    # dbClearResult(dbListResults(conn)[[1]])
    dbDisconnect(conn)
  }
}


do.fit <-
  function(cur_bid_str,
           cur_pid_str,
           cur.model.name,
           cur_fit_fun,
           fit_fun_arg_list) {
    # cur.train.size <- length(cur_train_y_ts)
    cur.model.na <- FALSE
    fit.start.time <- Sys.time()
    # 9: stop("series is not periodic or has less than two periods")
    # 8: stl(ts(x.bc, frequency = freq), "per")
    # 7: bld.mbb.bootstrap(y, 100)
    # 6: lapply(bootstrapped_series, function(x) {
    #   mod <- ets(x, ...)
    # })
    cur.model.fit <- NA
    tryCatch({
      # Error in stl(na.interp(xx), s.window = 7) :
      # series is not periodic or has less than two periods
      cur.model.fit <- do.call(cur_fit_fun, fit_fun_arg_list)
    }, error = function(e) {
      log4r::error(logger, sprintf("%s", as.character(e)))
      print(head(sys.calls()))
      cur.model.na <<- TRUE
    })
    fit.end.time <- Sys.time()
    if (!cur.model.na) {
      fit.time.taken <- fit.end.time - fit.start.time
      # cur.model.df <- model.df(cur.model.fit)
      # if (!is.na(cur.model.df)) {
      #   if (cur.model.df + 1 >= cur.train.size) {
      #     log4r::warn(
      #       logger,
      #       sprintf(
      #         "%s '%s' unable to fit %d point with %d parameters",
      #         cur_out_comm_prefix,
      #         cur.model.name,
      #         cur.train.size,
      #         cur.model.df
      #       )
      #     )
      #   }
      # }
    }
    else{
      fit.time.taken <- NA
    }
    log4r::info(
      logger,
      sprintf(
        "%s '%s' model fitting time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        format(fit.time.taken)
      )
    )
    # log4r::info(
    #   logger,
    #   sprintf(
    #     "%d points, %s '%s' model fitting time taken: %s",
    #     cur.train.size,
    #     cur_out_comm_prefix,
    #     cur.model.name,
    #     format(fit.time.taken)
    #   )
    # )
    return(
      list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur.model.na = cur.model.na,
        cur.model.fit = cur.model.fit,
        fit.time.taken = fit.time.taken
      )
    )
  }


do.predict <-
  function(cur_bid_str,
           cur_pid_str,
           cur.model.name,
           cur.model.fit,
           cur_predict_fun,
           predict_fun_arg_list) {
    if (!is.null(cur.model.fit) && !is.na(cur.model.fit)) {
      cur.model.pred <-
        do.call(cur_predict_fun, args = c(list(object = cur.model.fit), predict_fun_arg_list))  # forecast with intervals
      if (is.element("list", class(cur.model.pred))) {
        cur.pred.values <- cur.model.pred$mean
      } else
        # vector
        cur.pred.values <- cur.model.pred
    }
    else{
      cur.pred.values <- rep(NA, hold_out_h)
    }
    
    return(list(cur.pred.values = cur.pred.values))
  }


do.fit.predict <-
  function(cur_bid_str,
           cur_pid_str,
           cur.model.name,
           cur_fit_fun,
           fit_fun_arg_list,
           cur_predict_fun,
           predict_fun_arg_list) {
    # cur.train.size <- length(cur_train_y_ts)
    cur.model.na <- FALSE
    fit.start.time <- Sys.time()
    # 9: stop("series is not periodic or has less than two periods")
    # 8: stl(ts(x.bc, frequency = freq), "per")
    # 7: bld.mbb.bootstrap(y, 100)
    # 6: lapply(bootstrapped_series, function(x) {
    #   mod <- ets(x, ...)
    # })
    cur.model.fit <- NA
    tryCatch({
      # Error in stl(na.interp(xx), s.window = 7) :
      # series is not periodic or has less than two periods
      cur.model.fit <- do.call(cur_fit_fun, fit_fun_arg_list)
    }, error = function(e) {
      log4r::error(logger, sprintf("%s", as.character(e)))
      print(head(sys.calls()))
      cur.model.na <<- TRUE
    })
    fit.end.time <- Sys.time()
    if (!cur.model.na) {
      fit.time.taken <- fit.end.time - fit.start.time
      # cur.model.df <- model.df(cur.model.fit)
      # if (!is.na(cur.model.df)) {
      #   if (cur.model.df + 1 >= cur.train.size) {
      #     log4r::warn(
      #       logger,
      #       sprintf(
      #         "%s '%s' unable to fit %d point with %d parameters",
      #         cur_out_comm_prefix,
      #         cur.model.name,
      #         cur.train.size,
      #         cur.model.df
      #       )
      #     )
      #   }
      # }
      cur.model.pred <-
        do.call(cur_predict_fun, args = c(list(object = cur.model.fit), predict_fun_arg_list))  # forecast with intervals
      if (is.element("forecast", class(cur.model.pred))) {
        cur.pred.values <- cur.model.pred$mean
      } else
        # vector
        cur.pred.values <- cur.model.pred
    }
    else{
      fit.time.taken <- NA
      cur.pred.values <- rep(NA, hold_out_h)
    }
    log4r::info(
      logger,
      sprintf(
        "%s '%s' model fitting time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        format(fit.time.taken)
      )
    )
    # log4r::info(
    #   logger,
    #   sprintf(
    #     "%d points, %s '%s' model fitting time taken: %s",
    #     cur.train.size,
    #     cur_out_comm_prefix,
    #     cur.model.name,
    #     format(fit.time.taken)
    #   )
    # )
    return(
      list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur.model.na = cur.model.na,
        cur.model.fit = cur.model.fit,
        fit.time.taken = fit.time.taken,
        cur.pred.values = cur.pred.values
      )
    )
  }


# output dir set up
out_dir <- "out"
dput_file_dir <- sprintf("%s/dput/CV", out_dir)
dir.create(dput_file_dir, showWarnings = FALSE, recursive = TRUE)
error_file_dir <- sprintf("%s/error/CV", out_dir)
dir.create(error_file_dir,
           showWarnings = FALSE,
           recursive = TRUE)
png_file_dir <- sprintf("%s/png/CV", out_dir)
dir.create(png_file_dir, showWarnings = FALSE, recursive = TRUE)
gif_file_dir <- sprintf("%s/gif/CV", out_dir)
dir.create(gif_file_dir, showWarnings = FALSE, recursive = TRUE)
stat_file_dir <- sprintf("%s/stat/CV", out_dir)
dir.create(stat_file_dir, showWarnings = FALSE, recursive = TRUE)

vv_stat_days_count <- 30
hourly.vv.earliest.date <-
  as.Date(strptime("2017-05-05", "%Y-%m-%d"))

# https://www.timeanddate.com/holidays/china/
holidays_df <-
  read.table(
    "~/R_BoxCox_arima_batch_prediction/chinese_holidays",
    sep = "\t",
    quote = "",
    header = TRUE
  )
holidays_df$Date <- as.Date(strptime(holidays_df$Date, "%Y %b %d"))

yesterday <- Sys.Date() - 1
yesterday_str <- format(yesterday, "%Y%m%d")
vv_start_date <- yesterday - vv_stat_days_count + 1
vv_start_date_str <- format(vv_start_date, "%Y%m%d")
vv_end_date_str <- yesterday_str
last_feasible_day <- yesterday


bid_str_list <- c("(9)", "(12)")  # aPhone, iPhone
bid_name_list <- c("aPhone", "iPhone")
names(bid_str_list) <- bid_name_list
bid_str_num <- length(bid_str_list)
total_task_num <- bid_str_num * top_pid_cnt
ill_date_pid_list <- c()
ill_date_pid_name_list <- c()
ill_date_pid_idx_list <- c()
skipped_pid_list <- c(c())
skipped_pid_name_list <- c(c())
skipped_pid_train_size_list <- c(c())
forecast_list <-
  c() # forecast value list
rmse_list <-
  c() # CV RMSE value list
mape_list <-
  c() # CV MAPE value list
clip_info_list <- c()
train_data_list <- c()
cv_res_list <- c()


hourly.vv.pg.drv <- NULL
daily.vv.pg.drv <- NULL
mysql.drv <- NULL

mean_abs_vibr_ratio_list <-
  NULL # MEAN ABSOLUTE VIBRATION RATIO of test ts

for (j in seq_along(bid_str_list)) {
  cur_bid_str <- bid_str_list[[j]]
  cur_bid_name <- bid_name_list[[j]]
  
  pid_vv_sql <-
    sprintf(
      "select pid, sum(vv) as vv from vv_pid_day where bid in %s and date between %s and %s and pid != -1 group by pid order by vv desc limit %d; ",
      cur_bid_str,
      vv_start_date_str,
      vv_end_date_str,
      top_pid_cnt
    )
  print(pid_vv_sql)
  
  
  # create a connection
  # save the password that we can "hide" it as best as we can by collapsing it
  pw <- {
    "Dqi2Si3EER_ja983KBDFg"
  }
  # loads the PostgreSQL driver
  daily.vv.pg.drv <- dbDriver("PostgreSQL")
  # creates a connection to the postgres database
  # note that "con" will be used later in each connection to the database
  pg.daily.vv.con <- dbConnect(
    daily.vv.pg.drv,
    dbname = "dm_result",
    host = "10.100.5.85",
    port = 2345,
    user = "analyse_readonly",
    password = pw
  )
  
  rm(pw) # removes the password
  
  # query the top pid from postgreSQL
  sql.start.time <- Sys.time()
  pid_vv_data <- dbGetQuery(pg.daily.vv.con, pid_vv_sql)
  sql.end.time <- Sys.time()
  sql.time.taken <- sql.end.time - sql.start.time
  
  pid_num <- length(pid_vv_data$vv)
  info(
    logger,
    sprintf(
      "查询%s %s-%s %ddays top %d 合集VV, 耗时:\n%s",
      cur_bid_name,
      vv_start_date_str,
      vv_end_date_str,
      vv_stat_days_count,
      top_pid_cnt,
      format(sql.time.taken)
    )
  )
  pid_str_list <- as.character(pid_vv_data$pid)
  # pid_str_list <-
  #   c("309556",
  #     "319929",
  #     "319123",
  #     "320515",
  #     "299437",
  #     "320520",
  #     "321502")
  # pid_str_list <-  c(
  #    "320519",
  #    "320520",
  #    "320640",
  #    "321400",
  #    "321502",
  #    "321779",
  #    "299437",
  #    "308710",
  #    "313552"
  #  )
  
  mysql.drv <- MySQL()
  mysql.db = dbConnect(
    mysql.drv,
    user = 'bigdata_r',
    password = 'B1gD20t0_r',
    port = 3306,
    host = '10.27.106.230',
    dbname = "mgmd"
  )
  dbSendQuery(mysql.db, "SET NAMES utf8")
  
  
  cur_pid_sql_cond_str <-
    paste0('(', paste(pid_str_list, collapse = ","), ')')
  sql <-
    sprintf(
      'select clipId, clipName, updateTime, releaseTime  from asset_v_clips where clipId in %s;',
      cur_pid_sql_cond_str
    )
  print(sql)
  clip_info_list[[cur_bid_str]] <- dbGetQuery(mysql.db, sql)
  clip_info_list[[cur_bid_str]]$updateTime <-
    strptime(clip_info_list[[cur_bid_str]]$updateTime, format = "%Y-%m-%d %H:%M:%S")
  clip_info_list[[cur_bid_str]]$releaseTime <-
    strptime(clip_info_list[[cur_bid_str]]$releaseTime, format = "%Y-%m-%d %H:%M:%S")
  # release_date <- as.Date(clip_info_list[[cur_bid_str]]$releaseTime)  # this is NOT accurate within DB table
  
  clip_startTime_sql <-
    paste(
      sprintf(
        "select b.clipId, a.startTime from (select a.clipId, min(updateTime) as startTime from asset_v_parts a where clipId in %s and isIntact = 1 group by clipId) a",
        cur_pid_sql_cond_str
      )
      ,
      "inner join asset_v_clips b on a.clipId = b.clipId;"
    )
  print(clip_startTime_sql)
  pid_startTime_df <- dbGetQuery(mysql.db, clip_startTime_sql)
  pid_startTime_df$startTime <-
    strptime(pid_startTime_df$startTime, format = "%Y-%m-%d %H:%M:%S")
  # match pid order of two query
  clip_info_df <-
    merge(clip_info_list[[cur_bid_str]], pid_startTime_df, by = "clipId")
  clip_info_df$releaseTime <-
    NULL # this mislabeling date is corrected by startTime
  start_date <- as.Date(clip_info_df$startTime)
  start_date <-
    as.Date(sapply(start_date, function(cur.date)
      ifelse(
        cur.date > hourly.vv.earliest.date,
        cur.date,
        hourly.vv.earliest.date
      )))
  end_date <- as.Date(clip_info_df$updateTime)
  pid_name_list <- clip_info_df$clipName
  pid_name_list <-
    gsub(" ", "", pid_name_list, fixed = TRUE)  # remove white spaces
  pid_str_list <- as.character(clip_info_df$clipId)
  names(pid_str_list) <- pid_name_list
  # stopifnot(start_date < end_date)
  if (any(start_date >= end_date)) {
    ill_date_idx <- which(start_date >= end_date)
    ill_date_pid_idx_list[[cur_bid_str]] <- ill_date_idx
    ill_date_pid_name_list[[cur_bid_str]] <-
      pid_name_list[ill_date_idx]
    ill_date_pid_list[[cur_bid_str]] <- pid_str_list[ill_date_idx]
    names(ill_date_pid_list[[cur_bid_str]]) <-
      pid_name_list[ill_date_idx]
    for (ill_pid_i in ill_date_idx) {
      ill_date_pid <- pid_str_list[ill_pid_i]
      ill_date_pid_name <- pid_name_list[ill_pid_i]
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(ill_date_pid))
      skipped_pid_name_list[[cur_bid_str]] <-
        c(skipped_pid_name_list[[cur_bid_str]], list(ill_date_pid_name))
      skipped_pid_train_size_list[[cur_bid_str]] <-
        c(skipped_pid_train_size_list[[cur_bid_str]], list(NA))
    }
  }
  start_date_str_list <-
    format(start_date, format = "%Y%m%d")
  
  # predict_end_date <- end_date + hold_out_h
  if (is.null(max_train_size)) {
    predict_end_date <- end_date + hold_out_h
  } else {
    predict_end_date <-
      start_date + (max_train_size - 1) + hold_out_h
  }
  predict_end_date <-
    as.Date(sapply(predict_end_date, function(x)
      ifelse(x > last_feasible_day, last_feasible_day, x)))  # min function not working with Date
  pred_end_date_str_list <-
    format(predict_end_date, format = "%Y%m%d")
  
  
  clip_updateTime_list_sql <-
    sprintf(
      "select a.clipId, GROUP_CONCAT(updateTime) as updateTime_list_str, max(updateTime) as updateTime from asset_v_parts a where clipId in %s and isIntact = 1 group by clipId;",
      cur_pid_sql_cond_str
    )
  clip_time_list_df <-
    dbGetQuery(mysql.db, clip_updateTime_list_sql)
  updateTime_list_list <-
    strsplit(clip_time_list_df$updateTime_list_str, ",", fixed = TRUE)
  updateTime_uniq_list_list <-
    sapply(updateTime_list_list, function(x)
      strptime(unique(unlist(x)), format = "%Y-%m-%d %H:%M:%S"))
  updateTime_uniq_list_list <-
    sapply(updateTime_uniq_list_list, function(x)
      x[order(x)])
  names(updateTime_uniq_list_list) <-
    as.character(clip_time_list_df$clipId)
  
  
  # loads the PostgreSQL driver
  hourly.vv.pg.drv <- dbDriver("PostgreSQL")
  # create a connection
  # save the password that we can "hide" it as best as we can by collapsing it
  pw <- {
    "Woiuksj982djlp09t9G2djS"
  }
  # creates a connection to the postgres database
  # note that "con" will be used later in each connection to the database
  hourly.vv.pg.con <- dbConnect(
    hourly.vv.pg.drv,
    dbname = "vv_hour_dm_result",
    host = "10.100.5.43",
    port = 2345,
    user = "readqueryplatform",
    password = pw
  )
  
  remove(pw)
  
  for (i in seq_along(pid_str_list)) {
    if (i %in% ill_date_pid_idx_list[[cur_bid_str]]) {
      ill_date_pid_name <-
        ill_date_pid_name_list[[cur_bid_str]][[which(ill_date_pid_idx_list[[cur_bid_str]] == i)]]
      error(
        logger = logger,
        sprintf(
          "%s '%s' update start date and end date overlapped!",
          cur_bid_name,
          ill_date_pid_name
        )
      )
      next
    }
    cur_start_date_str <- start_date_str_list[i]
    cur_pred_end_date_str <- pred_end_date_str_list[i]
    cur_pid_days_count = predict_end_date[i] - start_date[i] + 1
    cur_pid_str <- pid_str_list[i]
    cur_pid_name <- pid_name_list[i]
    cur_pid_num <- length(pid_str_list)
    
    cur_out_comm_prefix <-
      sprintf("%s '%s'", cur_bid_name, cur_pid_name)
    out_file_desc_prefix <-
      sprintf(
        "%s_%s_%s-%s_%d-points",
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        cur_pid_days_count
      )
    
    cur_hourly_vv_df <- NULL
    # pid_life_span <- seq(start_date[[i]], end_date[[i]], by = 1) # not a vector, not working
    cur_pid_query_date <- start_date[[i]]
    hourly.vv.start.time <- Sys.time()
    while (cur_pid_query_date <= predict_end_date[[i]]) {
      # https://stackoverflow.com/questions/32610271/looping-over-dates-with-r#32611668
      if (cur_pid_query_date < hourly.vv.earliest.date) {
        cur_pid_query_date = cur_pid_query_date + 1
        next
      }
      stopifnot(is.element("Date", class(cur_pid_query_date)))
      cur_date_str <- format(cur_pid_query_date, "%Y%m%d")
      cur.date.vv.sql <-
        sprintf(
          "select pid, date, sum(vv) as VV from vv_pid_hour_%s where bid in %s and pid in (%s) group by pid, date order by date",
          cur_date_str,
          cur_bid_str,
          cur_pid_str
        )
      cur.fetch.na <- FALSE
      tryCatch({
        cur_date_df <- dbGetQuery(hourly.vv.pg.con, cur.date.vv.sql)
      }, error = function(e) {
        error(logger, as.character(e))
        print(sys.calls())
        cur.fetch.na <<- TRUE
      })
      if (!cur.fetch.na) {
        cur_date_df$time <-
          strptime(cur_date_df$date, format = "%Y%m%d%H")
        cur_hourly_vv_df <- rbind(cur_hourly_vv_df, cur_date_df)
      }
      cur_pid_query_date = cur_pid_query_date + 1
      if (cur.fetch.na &&
          cur_pid_query_date <= predict_end_date[[i]]) {
        cur_start_date_str <- format(cur_pid_query_date, "%Y%m%d")
      }
    }
    hourly.vv.end.time <- Sys.time()
    hourly.vv.time.taken <-
      hourly.vv.end.time - hourly.vv.start.time
    cur_task_cnt <- (j - 1) * top_pid_cnt + i
    info(
      logger,
      sprintf(
        "%d(%.2f%%).\t%s 查询%d天逐小时VV耗时:%s",
        i,
        (cur_task_cnt * 1.0) / total_task_num * 100,
        cur_out_comm_prefix,
        cur_pid_days_count,
        format(hourly.vv.time.taken)
      )
    )
    cur_hourly_vv_df$nWeek = as.integer(strftime(cur_hourly_vv_df$time, "%W"))
    cur_hourly_vv_df$nDOY = as.integer(strftime(cur_hourly_vv_df$time, "%j"))
    cur_hourly_vv_df$dayofweek = as.integer(cur_hourly_vv_df$time$wday)
    cur_hourly_vv_df$hourofday = as.integer(format(cur_hourly_vv_df$time, "%H"))
    cur_hourly_vv_df$tt <- 1:length(cur_hourly_vv_df$vv)
    cur_hourly_vv_df$Date <-
      as.Date(strptime(as.character(cur_hourly_vv_df$date), format = "%Y%m%d"))
    cur_hourly_vv_df$is_holiday <-
      cur_hourly_vv_df$Date %in% holidays_df$Date
    
    
    # ggplot(data = cur_hourly_vv_df , aes(x = dayofweek, y = hourofday)) +
    #   geom_raster(aes(fill = vv), interpolate = TRUE) +
    #   scale_fill_gradient2(
    #     low = "navy",
    #     mid = "white",
    #     high = "red",
    #     midpoint = 0,
    #     limits = range(cur_hourly_vv_df$vv)
    #   ) +
    #   theme_classic() + labs(title = sprintf("%s 小时VV热力图", out_file_desc_prefix))
    
    cur_pid_updateTime <- updateTime_uniq_list_list[[cur_pid_str]]
    cur_pid_is_update_hour <-
      format(cur_hourly_vv_df$time, "%Y%m%d %H") %in% format(cur_pid_updateTime, "%Y%m%d %H")
    cur_pid_updateTime_single <-
      1 == length(unique(as.Date(cur_pid_updateTime)))
    update_time_single_list[[cur_bid_str]][[cur_pid_str]] <-
      list(cur_pid_updateTime_single)
    
    cur_sample_size <- nrow(cur_hourly_vv_df)
    if (cur_sample_size <= 2 * y_freq) {
      warn(
        logger,
        sprintf(
          "%s total VV count %d is less than %d, skipped",
          cur_out_comm_prefix,
          cur_sample_size,
          2 * y_freq
        )
      )
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(cur_pid_str))
      skipped_pid_name_list[[cur_bid_str]] <-
        c(skipped_pid_name_list[[cur_bid_str]], list(cur_pid_name))
      skipped_pid_train_size_list[[cur_bid_str]] <-
        c(skipped_pid_train_size_list[[cur_bid_str]], list(cur_sample_size))
      next
    }
    # y <- rnorm(max_sample_size, mean = test_mean, sd = test_sd)
    require("xts")
    y_xts <-
      xts(cur_hourly_vv_df$vv,
          frequency = y_freq,
          # not effective
          order.by = cur_hourly_vv_df$time)
    attr(y_xts, 'frequency') <-
      y_freq  # https://stackoverflow.com/questions/28744218/set-frequency-in-xts-object#39731116
    y_ts <- as.ts(y_xts)
    # y_ts <- ts(rnorm(max_sample_size, test_mean, test_sd) + 1:max_sample_size + 20*sin(2*pi*(1:max_sample_size)/y_freq), frequency = y_freq)
    real_max_train_size <-
      min(max_train_size, length(y_ts) - hold_out_h)
    cur_train_h_seq <-
      seq(2 * y_freq, real_max_train_size, by = hold_out_h)
    cur_CV_cnt <- length(cur_train_h_seq)
    cur_last_k_fold_stat_valid <- TRUE
    if (cur_CV_cnt < last_k_fold_stat_num) {
      cur_last_k_fold_stat_valid <- FALSE
    }
    cv_pred_end_date <-
      cur_hourly_vv_df$Date[real_max_train_size]
    cur_pred_end_date_str <- format(cv_pred_end_date, "%Y%m%d")
    cur_pid_days_count = cv_pred_end_date - start_date[i] + 1
    # update out file common description date
    out_file_desc_prefix <-
      sprintf(
        "%s_%s_%s-%s_%d-points",
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        cur_pid_days_count
      )
    if (real_max_train_size <= 2 * y_freq) {
      skipped_pid_list[[cur_bid_str]] <- c(skipped_pid_list[[cur_bid_str]], list(cur_pid_str))
      skipped_pid_name_list[[cur_bid_str]] <-
        c(skipped_pid_name_list[[cur_bid_str]], list(cur_pid_name))
      skipped_pid_train_size_list[[cur_bid_str]] <-
        c(skipped_pid_train_size_list[[cur_bid_str]],
          list(real_max_train_size))
      warn(
        logger,
        sprintf(
          "%s max train sample size %d is less than %d, skipped",
          cur_out_comm_prefix,
          real_max_train_size,
          2 * y_freq
        )
      )
      next
    }
    
    
    # plot update time vertical lines
    cur_plot_ts <- ts(cur_hourly_vv_df$vv, frequency = y_freq)
    cur_vv_update_plot_df <-
      data.frame(y = cur_hourly_vv_df$vv,
                 x = cur_hourly_vv_df$time,
                 panel = "Hourly VV")
    
    cur_chr_ratio_plot_df = data.frame(
      x = cur_hourly_vv_df$time,
      y = cur_hourly_vv_df$vv / lag(as.numeric(cur_plot_ts), 1) - 1,
      panel = "VV Change Ratio"
    )
    cur_plot_panel_df <-
      rbind(cur_vv_update_plot_df, cur_chr_ratio_plot_df)
    real_plot_panel_df <-
      subset(cur_plot_panel_df, x <= cv_pred_end_date)
    
    cols <- gg_color_hue(2)
    p <-
      ggplot(data = real_plot_panel_df, aes(x = x, y = y)) +
      facet_grid(panel ~ ., scales = "free") +
      geom_line(data = subset(cur_vv_update_plot_df, x <= cv_pred_end_date)) +
      geom_point(data = subset(cur_vv_update_plot_df, x <= cv_pred_end_date)) +
      geom_vline(
        data = data.frame(xint = cur_pid_updateTime),
        aes(xintercept = as.numeric(xint)),
        linetype = 4,
        colour = "red"
      ) +
      geom_col(
        data = subset(
          cur_chr_ratio_plot_df,!is.na(cur_chr_ratio_plot_df$y) &
            x <= cv_pred_end_date
        ),
        color = cols[[2]]
      ) +
      # geom_point(data = subset(cur_chr_ratio_plot_df,!is.na(cur_chr_ratio_plot_df$y))) +
      # scale_fill_manual(labels = c("Hourly Video Visit", "VV change ratio"), # not working
      #                   values = cols) +
      scale_x_datetime(
        breaks = date_breaks("12 hour"),
        minor_breaks = date_breaks("4 hour"),
        # UPPER case is NOT allowed within date_breaks
        labels = date_format("%Y-%m-%d %H:%M"),
        expand = c(0, 0)  # remove the whitespace on the left and right of the graph
      ) + xlab("日期") +
      theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
      ggtitle(sprintf("%s 合集逐小时VV及更新时间", cur_out_comm_prefix))
    
    # for (cur_time_i in seq_along(cur_pid_updateTime)) {
    #   print(cur_pid_updateTime[cur_time_i])
    #   p <- p + geom_vline(aes(xintercept = as.numeric(cur_pid_updateTime[cur_time_i])),
    #                       linetype = 4,
    #                       colour = "red")
    # }
    ggsave(
      sprintf("%s/%s_VV.png", png_file_dir, out_file_desc_prefix),
      width = 12,
      height = 6
    )
    
    # begin parallel CV
    require("iterators")
    require("doParallel")
    avail.cores <- detectCores()
    cl.cores <- floor(avail.cores * 0.75)
    real_clus_cores <- min(cl.cores, cur_CV_cnt)
    cl <-
      makeCluster(real_clus_cores, outfile = "")  # https://stackoverflow.com/questions/10903787/how-can-i-print-when-using-dopar/15078540#15078540
    registerDoParallel(cl)
    
    
    # y_ts <- ts(rnorm(max_sample_size,0,3) + 1:max_sample_size + 20*sin(2*pi*(1:max_sample_size)/y_freq), frequency=y_freq) # default monthly data
    for (train_h in cur_train_h_seq) {
      # test_y_ts <-
      #   window(y_ts, train_h, train_h + hold_out_h - 1)
      test_rng <- (train_h + 1):(train_h + hold_out_h)
      test_y_ts <-
        ts(y_ts[test_rng], start = time(y_ts)[test_rng], frequency = y_freq)
      cur_train_h_str <- as.character(train_h)
      test_ts_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]] <-
        test_y_ts
      test_y_xts <- y_xts[test_rng]
      test_xts_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]] <-
        test_y_xts
      # y <- ts(rnorm(14), frequency = y_freq)
      plot_color_list <- c()
      # for (cur_inc_cnt in 1:hold_out_h) {
      cur_train_rng <- 1:train_h
      # cur_train_rng <- 1:(train_h + cur_inc_cnt - 1)
      cur_train_y_ts <-
        ts(y_ts[cur_train_rng], start = time(y_ts)[cur_train_rng], frequency = y_freq) # window function report 'Error in window.default(x, ...) : 'start' cannot be after 'end''
      cur_train_y_xts <- y_xts[cur_train_rng]
      cur_test_rng <- (train_h + 1):(train_h + hold_out_h)
      # cur_test_rng <- train_h + cur_inc_cnt
      cur_test_y_ts <-
        ts(y_ts[cur_test_rng], start = time(y_ts)[cur_test_rng], frequency = y_freq)
      cur_test_y_xts <- y_xts[cur_test_rng]
      cur_test_y_vec <- as.numeric(cur_test_y_ts)
      # cur_train_y <- head(y, -cur_hold_out)
      # cur_test_y <- tail(y, cur_hold_out)
      cur_train_size <- length(cur_train_y_ts)
      cur_cv_is_update_hour_train_dummy <-
        cur_pid_is_update_hour[cur_train_rng]
      cur_cv_is_update_hour_test_dummy <-
        cur_pid_is_update_hour[cur_test_rng]
      if (cur_train_size > 2 * y_freq &&
          cur_train_size == real_max_train_size) {
        # seasonplot(cur_train_y_ts)
        cur_bc_seas_ratio <-
          var(stl(cur_train_y_bc_ts, s.window = "periodic")$time.series[, "seasonal"]) / var(cur_train_y_bc_ts)
        ggseasonplot(cur_train_y_bc_ts) + ggtitle(
          sprintf(
            "%s Box-Cox transformed Hourly VV seasonal plot",
            out_file_desc_prefix
          ),
          subtitle = sprintf(
            "var(STL seasonal component)/var(ts): %.2f%%",
            cur_bc_seas_ratio * 100
          )
        )
        ggsave(
          sprintf(
            "%s/%s_bc_season_plot.png",
            png_file_dir,
            out_file_desc_prefix
          ),
          width = 12,
          height = 6
        )
      }
      cur_max_lag <-
        ifelse(cur_train_size - 2 > max_lag_allow,
               max_lag_allow,
               cur_train_size - 2)
      lambda <- BoxCox.lambda(na.contiguous(cur_train_y_ts))
      cur_train_y_bc_ts <- BoxCox(cur_train_y_ts, lambda = lambda)
      cur_train_datetime <- index(cur_train_y_xts)
      cur_test_datetime <- index(cur_test_y_xts)
      has_weekly_data <- FALSE
      if (cur_train_size > 7 * 24) {
        has_weekly_data <- TRUE
      }
      # take note of mean vibration ratio of test data
      cur_test_y_chr_ratio <-
        as.numeric(cur_test_y_ts) / lag(as.numeric(cur_test_y_ts), 1) - 1
      cur_test_mean_abs_vibr_ratio <-
        mean(abs(cur_test_y_chr_ratio), na.rm = TRUE)
      mean_abs_vibr_ratio_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]] <-
        c(mean_abs_vibr_ratio_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]],
          list(cur_test_mean_abs_vibr_ratio))
      
      train_msts_four_reg <- NULL
      test_msts_four_reg <- NULL
      cur_train_y_msts <- NULL
      cur_train_y_bc_msts <- NULL
      if (cur_train_size > 24 * 7) {
        # weekly
        if (cur_train_size > 24 * 365) {
          # yearly
          cur_train_y_msts <-
            msts(cur_train_y_ts,
                 seasonal.periods = c(24, 24 * 7, 24 * 365.25)) # daily, weekly, yearly seasonality
          cur_train_y_bc_msts <-
            msts(cur_train_y_bc_ts,
                 seasonal.periods = c(24, 24 * 7, 24 * 365.25)) # daily, weekly, yearly seasonality
          # fourier_K <- c(12, 84, 4383)
          fourier_K <-
            c(floor(24 / 2), floor(24 * 7 / 2), floor(24 * 365.25 / 2)) # using higherst possible K might overfit when AICc is more approriate but the latter is time consuming
        } else {
          cur_train_y_msts <-
            msts(cur_train_y_ts, seasonal.periods = c(24, 24 * 7)) # daily, weekly seasonality
          cur_train_y_bc_msts <-
            msts(cur_train_y_bc_ts, seasonal.periods = c(24, 24 * 7)) # daily, weekly seasonality
          fourier_K <- c(floor(24 / 2), floor(24 * 7 / 2))
        }
      } else{
        cur_train_y_msts <- cur_train_y_ts
        cur_train_y_bc_msts <- cur_train_y_bc_ts
      }
      
      # start.time <- Sys.time()
      # # return value is INDEPENDENT of ts value and only the length of ts will be considered by fourier function
      # # https://github.com/robjhyndman/forecast/blob/8826e627a1d0e3cfa9cd8e01e17c912cfe40625c/R/season.R#L251
      # train_msts_four_reg <-
      #   fourier(cur_train_y_msts, K = fourier_K)
      # end.time <- Sys.time()
      # time.taken <- end.time - start.time
      # info(
      #   logger,
      #   sprintf(
      #     "%s multiple seasonal fourier xreg fitting time taken %s",
      #     cur_out_comm_prefix,
      #     format(time.taken)
      #   )
      # )
      # start.time <- Sys.time()
      # test_msts_four_reg <-
      #   fourier(cur_train_y_msts, K = fourier_K, h = hold_out_h)
      # end.time <- Sys.time()
      # time.taken <- end.time - start.time
      # info(
      #   logger,
      #   sprintf(
      #     "%s multiple seasonal fourier future xreg fitting time taken %s",
      #     cur_out_comm_prefix,
      #     format(time.taken)
      #   )
      # )
      
      start.time <- Sys.time()
      bestfit <- sel.fourier.K(cur_train_y_msts)
      end.time <- Sys.time()
      time.taken <- end.time - start.time
      info(
        logger,
        sprintf(
          "%s multiple seasonal fourier xreg selecting time taken %s",
          cur_out_comm_prefix,
          format(time.taken)
        )
      )
      train_msts_four_reg <-
        fourier(cur_train_y_msts, K = bestfit$K)
      train_four_reg <- train_msts_four_reg
      test_msts_four_reg <-
        fourier(cur_train_y_msts, K = bestfit$K, h = hold_out_h)
      test_four_reg <- test_msts_four_reg
      
      # Box-Cox version fourier terms
      start.time <- Sys.time()
      bestfit <- sel.fourier.K(cur_train_y_bc_msts)
      end.time <- Sys.time()
      time.taken <- end.time - start.time
      info(
        logger,
        sprintf(
          "%s Box-Cox multiple seasonal fourier xreg selecting time taken %s",
          cur_out_comm_prefix,
          format(time.taken)
        )
      )
      train_bc_msts_four_reg <-
        fourier(cur_train_y_bc_msts, K = bestfit$K)
      train_bc_four_reg <- train_bc_msts_four_reg
      test_bc_msts_four_reg <-
        fourier(cur_train_y_bc_msts, K = bestfit$K, h = hold_out_h)
      test_bc_four_reg <- test_bc_msts_four_reg
      
      train_holiday_dummy <-
        cur_hourly_vv_df$is_holiday[cur_train_rng]
      cur_holiday_dummy_valid <- any(train_holiday_dummy)
      test_holiday_dummy <-
        cur_hourly_vv_df$is_holiday[cur_test_rng]
      
      cur_update_hour_dummy_valid <-
        any(cur_cv_is_update_hour_train_dummy)
      train_update_hour_dummy <- cur_cv_is_update_hour_train_dummy
      test_update_hour_dummy <- cur_cv_is_update_hour_test_dummy
      
      train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]] <-
        list(
          cur_train_rng = cur_train_rng,
          cur_train_y_ts = cur_train_y_ts,
          cur_train_y_xts = cur_train_y_xts,
          cur_train_size = cur_train_size,
          cur_test_rng = cur_test_rng,
          cur_test_y_ts = cur_test_y_ts,
          cur_test_y_xts = cur_test_y_xts,
          cur_test_y_vec = cur_test_y_vec,
          lambda = lambda,
          cur_train_y_bc_ts = cur_train_y_bc_ts,
          cur_train_datetime = cur_train_datetime,
          cur_test_datetime = cur_test_datetime,
          cur_max_lag = cur_max_lag,
          has_weekly_data = has_weekly_data,
          train_four_reg = train_four_reg,
          test_four_reg = test_four_reg,
          train_bc_four_reg = train_bc_four_reg,
          test_bc_four_reg = test_bc_four_reg,
          cur_holiday_dummy_valid = cur_holiday_dummy_valid,
          train_holiday_dummy = train_holiday_dummy,
          test_holiday_dummy = test_holiday_dummy,
          cur_update_hour_dummy_valid = cur_update_hour_dummy_valid,
          train_update_hour_dummy = train_update_hour_dummy,
          test_update_hour_dummy = test_update_hour_dummy
        )
    } # for every CV size
    # train_four_reg <- NULL
    # for (cur_train_four_reg in train_fourier_list) {
    #   train_four_reg <- cbind(train_four_reg, cur_train_four_reg)
    # }
    # test_four_reg <- NULL
    # for (cur_test_four_reg in test_fourier_list) {
    #   test_four_reg <- cbind(test_four_reg, cur_test_four_reg)
    # }
    
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_rng = cur_train_info$cur_train_rng
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_train_y_xts = cur_train_info$cur_train_y_xts
      cur_train_size = cur_train_info$cur_train_size
      cur_test_rng = cur_train_info$cur_test_rng
      cur_test_y_ts = cur_train_info$cur_test_y_ts
      cur_test_y_xts = cur_train_info$cur_test_y_xts
      cur_test_y_vec = cur_train_info$cur_test_y_vec
      lambda = cur_train_info$lambda
      cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
      cur_train_datetime = cur_train_info$cur_train_datetime
      cur_test_datetime = cur_train_info$cur_test_datetime
      cur_max_lag = cur_train_info$cur_max_lag
      has_weekly_data = cur_train_info$has_weekly_data
      train_four_reg = cur_train_info$train_four_reg
      test_four_reg = cur_train_info$test_four_reg
      
      info(logger, "\n")
      #
      # require("forecast")
      # # cur.model.name <- "seasonal auto.ar"
      # cur.model.name <- "BoxCox auto.ar"
      # fit.start.time <- Sys.time()
      # cur.ar.fit <-
      #   auto.arima(
      #     cur_train_y_xts,
      #     max.p = cur_max_lag,
      #     max.d = 0,
      #     max.q = 0,
      #     max.order = cur_max_lag,
      #     seasonal = FALSE,
      #     # seasonal = TRUE,
      #     trace = verbose,
      #     stepwise = FALSE,
      #     # parallization of non-default ARIMA(p,d,q) is not supported
      #     lambda = lambda
      #   ) # dynamic ar model ONLY
      # # cur.arima.fit <-
      # #   auto.arima(
      # #     cur_train_y_ts,
      # #     max.p = cur_max_lag,
      # #     seasonal = FALSE,
      # #     trace = verbose,
      # #     stepwise = FALSE
      # #   )
      # fit.end.time <- Sys.time()
      # fit.time.taken <- fit.end.time - fit.start.time
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # info(
      #   logger,
      #   sprintf(
      #     "%d points, %s '%s' model fitting time taken: %s",
      #     cur_train_size,
      #     cur_out_comm_prefix,
      #     cur.model.name,
      #     format(fit.time.taken)
      #   )
      # )
      # cur.model.df <- length(cur.ar.fit$coef)
      # if (cur.model.df >= cur_train_size) {
      #   warn(
      #     logger,
      #     sprintf(
      #       "%s '%s' unable to fit %d point with %d parameters",
      #       cur_out_comm_prefix,
      #       cur.model.name,
      #       cur_train_size,
      #       cur.model.df
      #     )
      #   )
      # }
      # cur.ar.pred <-
      #   forecast(cur.ar.fit, h = hold_out_h, lambda = lambda)
      #
      # require("Metrics")
      # cur.pred.rmse <-
      #   rmse(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.ar.pred$mean))
      # cur.pred.mape <-
      #   mape(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.ar.pred$mean))
      # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.ar.pred$mean))
      # plot_color_list[[cur.model.name]] <-
      #   "blue"
      #
      # cur.model.name <- "BoxCox default auto.arima"
      # # cur_max_p <- cur_max_lag # slow as HELL
      # # max_q <- cur_max_lag # take FOREVER
      # # default values
      # max_p <- 5
      # max_q <- 5
      # max_P <- 2
      # max_Q = 2
      # cur_max_order <- max_p + max_q + max_P + max_Q
      # fit.start.time <- Sys.time()
      # cur.arima.fit <-
      #   auto.arima(
      #     cur_train_y_ts,
      #     max.p = max_p,
      #     max.order = cur_max_order,
      #     seasonal = FALSE,
      #     trace = verbose,
      #     stepwise = FALSE,
      #     lambda = lambda
      #   )
      # fit.end.time <- Sys.time()
      # fit.time.taken <- fit.end.time - fit.start.time
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # info(
      #   logger,
      #   sprintf(
      #     "%d points, %s '%s' model fitting time taken: %s",
      #     cur_train_size,
      #     cur_out_comm_prefix,
      #     cur.model.name,
      #     format(fit.time.taken)
      #   )
      # )
      # cur.model.df <- length(cur.arima.fit$coef)
      # if (cur.model.df >= cur_train_size) {
      #   warn(
      #     logger,
      #     sprintf(
      #       "%s '%s' unable to fit %d point with %d parameters",
      #       cur_out_comm_prefix,
      #       cur.model.name,
      #       cur_train_size,
      #       cur.model.df
      #     )
      #   )
      # }
      # cur.arima.pred <-
      #   forecast(cur.arima.fit, h = hold_out_h, lambda = lambda)
      # require("Metrics")
      # cur.pred.rmse <-
      #   rmse(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.arima.pred$mean))
      # cur.pred.mape <-
      #   mape(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.arima.pred$mean))
      # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arima.pred$mean))
      # plot_color_list[[cur.model.name]] <-
      #   "cyan"
      #
      # cur.model.name <- "BoxCox seasonal default auto.arima"
      # # cur_max_p <- cur_max_lag # slow as HELL
      # # max_q <- cur_max_lag # take FOREVER
      # # default values
      # max_p <- 5
      # max_q <- 5
      # max_P <- 2
      # max_Q = 2
      # cur_max_order <- max_p + max_q + max_P + max_Q
      # fit.start.time <- Sys.time()
      # cur.arima.fit <-
      #   auto.arima(
      #     cur_train_y_ts,
      #     # max.p = max_p,
      #     # max.order = cur_max_order,
      #     seasonal = TRUE,
      #     # trace = verbose,
      #     stepwise = FALSE,
      #     lambda = lambda
      #   )
      # fit.end.time <- Sys.time()
      # fit.time.taken <- fit.end.time - fit.start.time
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # info(
      #   logger,
      #   sprintf(
      #     "%d points, %s '%s' model fitting time taken: %s",
      #     cur_train_size,
      #     cur_out_comm_prefix,
      #     cur.model.name,
      #     format(fit.time.taken)
      #   )
      # )
      # cur.model.df <- length(cur.arima.fit$coef)
      # if (cur.model.df >= cur_train_size) {
      #   warn(
      #     logger,
      #     sprintf(
      #       "%s '%s' unable to fit %d point with %d parameters",
      #       cur_out_comm_prefix,
      #       cur.model.name,
      #       cur_train_size,
      #       cur.model.df
      #     )
      #   )
      # }
      # cur.arima.pred <-
      #   forecast(cur.arima.fit, h = hold_out_h, lambda = lambda)
      # require("Metrics")
      # cur.pred.rmse <-
      #   rmse(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.arima.pred$mean))
      # cur.pred.mape <-
      #   mape(as.numeric(cur_test_y_ts),
      #        as.numeric(cur.arima.pred$mean))
      # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arima.pred$mean))
      # plot_color_list[[cur.model.name]] <-
      #   "cyan"
      #
      #
      # cur.model.name <- "tslm"
      # if (ncol(train_four_reg) + 1 >= cur_train_size) {
      #   warn(
      #     logger,
      #     sprintf(
      #       "%s '%s' unable to fit %d point with %d parameters",
      #       cur_out_comm_prefix,
      #       cur.model.name,
      #       cur_train_size,
      #       ncol(train_four_reg) + 1
      #     )
      #   )
      #   require("usdm")
      #   v2 <- vifstep(train_four_reg, th = 10)
      #   sel_colnames <- as.character(v2@results$Variables)
      #   excluded_colnames <- as.character(v2@excluded)
      #   cur_filtered_xreg <- train_four_reg[, sel_colnames]
      #   if (length(excluded_colnames) > 0) {
      #     info(logger,
      #          sprintf(
      #            "\tpredictors '%s' is excluded",
      #            paste(excluded_colnames, collapse = ",")
      #          ))
      #   }
      #   train_four_reg <- cur_filtered_xreg
      #   test_four_reg <- test_four_reg[, sel_colnames, drop = FALSE]
      # }
      # fit.start.time <- Sys.time()
      # cur.model.fit <- tslm(cur_train_y_ts ~ train_four_reg)
      # fit.end.time <- Sys.time()
      # fit.time.taken <- fit.end.time - fit.start.time
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # info(
      #   logger,
      #   sprintf(
      #     "%d points, %s '%s' model fitting time taken: %s",
      #     cur_train_size,
      #     cur_out_comm_prefix,
      #     cur.model.name,
      #     format(fit.time.taken)
      #   )
      # )
      # cur.model.df <- model.df(cur.model.fit)
      # # cur.model.cv <- CV(cur.model.fit)
      # cur.lm.coef.p <-
      #   summary(cur.model.fit)$coefficients[, 4] # https://stackoverflow.com/questions/5587676/pull-out-p-values-and-r-squared-from-a-linear-regression#5588638
      # insig_pred_var_names <-
      #   names(cur.lm.coef.p[which(cur.lm.coef.p >= 0.05)])
      # if (length(insig_pred_var_names)) {
      #   warn(
      #     logger,
      #     sprintf(
      #       "%s train sample size %d,  '%s' predictors %d/%d(%.2f%%) '%s' is statistically insignificant",
      #       cur_out_comm_prefix,
      #       cur_train_size,
      #       cur.model.name,
      #       length(insig_pred_var_names),
      #       length(coef(cur.model.fit)),
      #       length(insig_pred_var_names) * 1.0 / length(coef(cur.model.fit)) * 100,
      #       paste(insig_pred_var_names, collapse = ",")
      #     )
      #   )
      # }
      # # Error in forecast.lm(tslm.fit, h = hold_out_h) :
      # # Variables not found in newdata
      # cur.model.pred <-
      #   predict(cur.model.fit, newx = test_four_reg)
      #
      # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred[1:hold_out_h]))
      # cur.pred.rmse <-
      #   rmse(as.numeric(cur_test_y_ts), as.numeric(cur.model.pred))
      # cur.pred.mape <-
      #   mape(as.numeric(cur_test_y_ts), as.numeric(cur.model.pred))
      # plot_color_list[[cur.model.name]] <-
      #   "yellow"
      #
      #
      # # # suck at CV test
      # # require("mgcv")
      # # cur.model.name <- "GAM"
      # # cur_decomp <- NULL
      # # tryCatch({
      # #   cur_decomp <-
      # #     stl(cur_train_y_ts,
      # #         s.window = "periodic",
      # #         robust = TRUE)
      # # }, error = function(e) {
      # #   error(logger, as.character(e))
      # #   print(sys.calls())
      # # })
      # # if (!is.null(cur_decomp)) {
      # #   gam_mat <-
      # #     data.frame(
      # #       y = as.numeric(cur_train_y_ts),
      # #       seasadj_y = as.numeric(seasadj(cur_decomp)),
      # #       nWeek = as.integer(strftime(cur_train_datetime, "%W")),
      # #       nDOY = as.integer(strftime(cur_train_datetime, "%j")),
      # #       dayofweek = as.integer(cur_train_datetime$wday),
      # #       hourofday = as.integer(format(cur_train_datetime, "%H")),
      # #       tt = cur_hourly_vv_df$tt[cur_train_rng]
      # #     )
      # # } else{
      # #   gam_mat <-
      # #     data.frame(
      # #       y = as.numeric(cur_train_y_ts),
      # #       nWeek = as.integer(strftime(cur_train_datetime, "%W")),
      # #       nDOY = as.integer(strftime(cur_train_datetime, "%j")),
      # #       dayofweek = as.integer(cur_train_datetime$wday),
      # #       hourofday = as.integer(format(cur_train_datetime, "%H")),
      # #       tt = cur_hourly_vv_df$tt[cur_train_rng]
      # #     )
      # # }
      # #
      # # m <- length(unique(gam_mat))
      # # cur_dayofweek_k <-
      # #   min(length(unique(gam_mat[["dayofweek"]])) - 1, m)
      # # cur_hourofday_k <-
      # #   min(length(unique(gam_mat[["hourofday"]])) - 1, m)
      # # cur_nWeek_k <- min(length(unique(gam_mat[["nWeek"]])) - 1, m)
      # # cur_nDOY_k <- min(length(unique(gam_mat[["nDOY"]])) - 1, m)
      # # ctrl <-
      # #   list(niterEM = 0,
      # #        msVerbose = TRUE,
      # #        optimMethod = "L-BFGS-B")
      # # fit.start.time <- Sys.time()
      # # if (has_weekly_data) {
      # #   cur_gam_fit <-
      # #     gam(
      # #       y ~ s(tt) +
      # #         te(
      # #           # https://stats.stackexchange.com/questions/234809/r-mgcv-why-do-te-and-ti-tensor-products-produce-different-surfaces#234817
      # #           dayofweek,
      # #           hourofday,
      # #           k = c(cur_dayofweek_k, cur_hourofday_k),
      # #           bs = c("cc", "cc")
      # #         ),
      # #       data = gam_mat,
      # #       ctrl = ctrl
      # #       # correlation = corARMA(
      # #       #   form = ~ 1,
      # #       #   p = y_freq #  Error in lme.formula(y.0 ~ X - 1, random = rand, data = strip.offset(mf),  :
      #                        #  nlminb problem, convergence error code = 1
      #                        # message = iteration limit reached without convergence (10)
      # #       # )
      # #       # correlation = corAR1(form = ~ 1)
      # #     )
      # #
      # #   # gamm(
      # #   #   # y ~ s(dayofweek, k = 7) + s(hourofday, k = 24), # A term has fewer unique covariate combinations than specified maximum degrees of freedom
      # #   #   y ~ s(dayofweek, k = cur_dayofweek_k, bs = "cc")
      # #   #   + s(hourofday, k = cur_hourofday_k, bs = "cc"),
      # #   #   data = gam_mat,
      # #   #   correlation = corAR1(form = ~ 1)
      # #   # )
      # #
      # # } else{
      # #   cur_gam_fit <-
      # #     gam(
      # #       # y ~ s(dayofweek, k = 7) + s(hourofday, k = 24), # A term has fewer unique covariate combinations than specified maximum degrees of freedom
      # #       y ~  s(tt) + s(hourofday, k = cur_hourofday_k, bs = "cc"),
      # #       data = gam_mat,
      # #       ctrl = ctrl
      # #       # correlation = corAR1(form = ~ 1)
      # #     )
      # # }
      # # fit.end.time <- Sys.time()
      # # fit.time.taken <- fit.end.time - fit.start.time
      # # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # # info(
      # #   logger,
      # #   sprintf(
      # #     "%d points, %s '%s' model R-sq.(adj) = %.4f, fitting time taken: %s",
      # #     cur_train_size,
      # #     cur_out_comm_prefix,
      # #     cur.model.name,
      # #     summary(cur_gam_fit)$r.sq,
      # #     format(fit.time.taken)
      # #   )
      # # )
      # # layout(matrix(1:2, nrow = 1))
      # # plot(cur_gam_fit, shade = TRUE)
      # # par(mfrow = c(1, 1))
      # # # vis.gam(cur_gam_fit, n.grid = 50, theta = 35, phi = 32, zlab = "",  ticktype = "detailed", color = "topo", main = "te(D, W)")
      # # # vis.gam(cur_gam_fit,  main = "te(D, W)", plot.type = "contour", color = "terrain", contour.col = "black", lwd = 2)
      #
      # #
      # # cur_gam_predict <-
      # #   predict(cur_gam_fit, type = "response", newdata = fxreg_gam_mat)
      # # cur.pred.mape <-
      # #   mape(as.numeric(cur_test_y_ts), cur_gam_predict)
      # # cur.pred.rmse <-
      # #   rmse(as.numeric(cur_test_y_ts), cur_gam_predict)
      # # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(as.numeric(cur_gam_predict)))
      #
      
      #
      # # cur.model.name <- "ARIMAX using fourier terms"
      # # cur_fit_na <- FALSE
      # # cur.model.na <- FALSE
      # # require("parallel")
      # # avail.cores <- floor(detectCores() / 2)
      # # fit.start.time <- Sys.time()
      # # tryCatch({
      # #   cur.model.fit <-
      # #     auto.arima(
      # #       cur_train_y_ts,
      # #       xreg = train_four_reg,
      # #       stepwise = FALSE,
      # #       parallel = TRUE,
      # #       num.cores = avail.cores # default value of this parameter is NOT effective
      # #     )
      # # }, error = function(e) {
      # #   error(logger, sprintf("%s", as.character(e)))
      # #   print(sys.calls())
      # #   cur.model.na <<- TRUE
      # # })
      # # fit.end.time <- Sys.time()
      # # fit.time.taken <-
      # #   fit.end.time - fit.start.time  # INCREDIBLY slower than other model with fourier predictors like tslm etc even with multiple CPU cores utilization.
      # # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # # c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # # info(
      # #   logger,
      # #   sprintf(
      # #     "%d points, %s '%s' model fitting time taken: %s",
      # #     cur_train_size,
      # #     cur_out_comm_prefix,
      # #     cur.model.name,
      # #     format(fit.time.taken)
      # #   )
      # # )
      # # cur.model.df <- model.df(cur.model.fit)
      # # if (cur.model.df >= cur_train_size) {
      # #   print(
      # #     sprintf(
      # #       "%s '%s' unable to fit %d point with %d parameters",
      # #       cur_out_comm_prefix,
      # #       cur.model.name,
      # #       cur_train_size,
      # #       cur.model.df
      # #     )
      # #   )
      # # }
      #
      # # if (!cur.model.na) {
      # #   cur.arimax.pred <-
      # #     forecast(cur.model.fit, xreg = test_four_reg, h = hold_out_h)
      # #
      # #   require("Metrics")
      # #   cur.pred.rmse <- rmse(cur_test_y_ts, cur.arimax.pred$mean)
      # #   forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # #     c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arimax.pred$mean))
      # # } else {
      # #   forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # #     c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(rep(NA, hold_out_h)))
      # # }
      #
      # # short_lm_model <- tslm(train_y_ts ~ train_four_reg)
      # # CV(short_lm_model)
      # # cur_lm_fit_name <-
      # #   paste(
      # #     coefficients(short_lm_model),
      # #     names(coefficients(short_lm_model)),
      # #     sep = "*",
      # #     collapse = "+"
      # #   )
      # # cur_lm_predict <-
      # #   predict(short_lm_model, newx = test_four_reg, h = hold_out_h)
      # # # cv <- mean((residuals(obj) / (1 - hatvalues(obj))) ^ 2, na.rm = TRUE)
      # # require("glmnet")
      # # cv_lasso_fit <- cv.glmnet(train_four_reg, train_y)
      # # # best_lambda <- lasso_fit$lambda[which.min(lasso_fit$lambda)]
      # # best_lambda <-
      # #   cv_lasso_fit$lambda.min # or cv_lasso_fit$lambda.1se
      # # lasso_fit <-
      # #   glmnet(train_four_reg, train_y, lambda = best_lambda)
      # # plot(lasso_fit)
      # # lasso_fcast <-
      # #   predict(
      # #     lasso_fit,
      # #     newx = test_four_reg,
      # #     lambda = best_lambda,
      # #     type = "coefficients",
      # #     h = hold_out_h
      # #   )
      # # plot(lasso_fcast, type = "b,c")
      # # lines(test_y, col = "red")
      # # require("Metrics")
      # # rmse(test_y, lasso_fcast)
      # # }
      
      # cat("\n")
      info(logger, "\n")
    } # for every CV train size
    
    
    cur_train_y_ts_list <- NULL
    cur_lambda_list <- NULL
    cur_train_y_bc_ts_list <- NULL
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
      cur_train_y_ts_list <-
        c(cur_train_y_ts_list, list(cur_train_y_ts))
      lambda <- cur_train_info$lambda
      cur_lambda_list <- c(cur_lambda_list, list(lambda))
      cur_train_y_bc_ts_list <-
        c(cur_train_y_bc_ts_list, list(cur_train_y_bc_ts))
    }
    cur_CV_cnt <- length(cur_train_h_seq)
    
    # ptime <-
    #   system.time({
    #     foreach (
    #       cur_train_y_ts = iter(cur_train_y_ts_list),
    #       .packages = c("log4r", "forecast"),
    #       .verbose = TRUE
    #     ) %dopar% {
    #       stopifnot(class(cur_train_y_ts) == "ts")
    #       args_list <- list(
    #         cur_bid_str = cur_bid_str,
    #         cur_pid_str = cur_pid_str,
    #         cur_train_y_ts = cur_train_y_ts,
    #         forecast_list = forecast_list,
    #         model_fit_time_list = model_fit_time_list
    #       )
    #       do.call(do.baggedETS.fit,
    #               args = args_list)
    #     }
    #   })[3]
    # print(format(ptime))
    #
    
    
    # get tsoutliers predictors from arima fitting for every CV size, PROHIBITIVE slow, takes approx 20 mins for 300+ data points
    if (using_tsoutliers) {
      ptime.start <- Sys.time()
      tsoutliers_res_list <- foreach(
        cur_train_y_ts = iter(cur_train_y_ts_list),
        i = icount(),
        .packages = c("tsoutliers", "log4r"),
        .verbose = TRUE
      ) %dopar% {
        do.call(tsoutliers.xreg, args = c(list(cur_train_y_ts), list(h = hold_out_h)))
      }
      ptime.end <- Sys.time()
      ptime.taken <- ptime.end - ptime.start
      info(
        logger,
        sprintf(
          "%s %d fold tsoutliers finding time taken: %s",
          cur_out_comm_prefix,
          cur_CV_cnt,
          format(ptime.taken)
        )
      )
      
      tsoutliers_xreg_list <- NULL
      for (tsoutliers_list in tsoutliers_res_list) {
        cur_train_h_str <- as.character(tsoutliers_list$cur.train.size)
        tsoutliers_xreg_list[[cur_train_h_str]] = list(xreg = tsoutliers_list$xreg, fxreg = tsoutliers_list$fxreg)
      }
      
      cur_fit_args_list <- NULL
      cur_fcast_args_list <- NULL
      cur.model.name <- "BoxCox NNAR using tsoutliers"
      for (train_h in cur_train_h_seq) {
        cur_train_h_str <- as.character(train_h)
        cur_train_info <-
          train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
        cur_train_y_ts <- cur_train_info$cur_train_y_ts
        cur.train.size <- length(cur_train_y_ts)
        lambda = cur_train_info$lambda
        cur_tsoutliers_na <- TRUE
        cur_tsoutliers_xreg <-
          tsoutliers_xreg_list[[cur_train_h_str]]$xreg
        stopifnot(class(cur_tsoutliers_xreg) == "matrix" ||
                    is.na(cur_tsoutliers_xreg))
        stopifnot(nrow(cur_tsoutliers_xreg) == cur.train.size)
        cur_tsoutliers_fxreg <-
          tsoutliers_xreg_list[[cur_train_h_str]]$fxreg
        stopifnot(nrow(cur_tsoutliers_fxreg) == hold_out_h)
        if (!is.na(cur_tsoutliers_xreg)) {
          cur_tsoutliers_na <- FALSE
          cur_fit_args_list <- c(cur_fit_args_list,
                                 list(
                                   list(
                                     y = cur_train_y_ts,
                                     lambda = lambda,
                                     xreg = cur_tsoutliers_xreg
                                   )
                                 ))
          cur_fcast_args_list <-
            c(cur_fcast_args_list,
              list(
                list(
                  h = hold_out_h,
                  lambda = lambda,
                  xreg = cur_tsoutliers_fxreg
                )
              ))
        }
        if (cur_tsoutliers_na) {
          cur_fit_args_list <- c(cur_fit_args_list,
                                 list(list(
                                   y = cur_train_y_ts,
                                   lambda = lambda
                                 )))
          cur_fcast_args_list <-
            c(cur_fcast_args_list, list(list(
              h = hold_out_h, lambda = lambda
            )))
        }
      }
      
      ptime.start <- Sys.time()
      cv_res_list <- foreach(
        cur_train_y_ts = iter(cur_train_y_ts_list),
        cur_fit_args = iter(cur_fit_args_list),
        cur_fcast_args = iter(cur_fcast_args_list),
        i = icount(),
        .packages = c("forecast", "log4r"),
        .verbose = TRUE
      ) %dopar% {
        args_list = list(
          cur_bid_str = cur_bid_str,
          cur_pid_str = cur_pid_str,
          cur.model.name = cur.model.name,
          cur_fit_fun = forecast::nnetar,
          fit_fun_arg_list = cur_fit_args,
          cur_predict_fun = forecast::forecast,
          predict_fun_arg_list = cur_fcast_args
        )
        do.call(do.fit.predict, args = args_list)
      }
      ptime.end <- Sys.time()
      ptime.taken <- ptime.end - ptime.start
      info(
        logger,
        sprintf(
          "%s '%s' %d fold CV time taken: %s",
          cur_out_comm_prefix,
          cur.model.name,
          cur_CV_cnt,
          format(ptime.taken)
        )
      )
      # collect results
      collect.cv.res(
        cur_train_h_seq = cur_train_h_seq,
        cv_res_list = cv_res_list,
        model_fit_list = model_fit_list,
        model_fit_time_list = model_fit_time_list,
        forecast_list = forecast_list,
        h = hold_out_h
      )
    }
    
    
    cur.model.name <- "BoxCox bsts"
    cur_fit_args_list <- NULL
    cur_fcast_args_list <- NULL
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_rng = cur_train_info$cur_train_rng
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_train_y_xts = cur_train_info$cur_train_y_xts
      cur_train_size = cur_train_info$cur_train_size
      cur_test_rng = cur_train_info$cur_test_rng
      cur_test_y_ts = cur_train_info$cur_test_y_ts
      cur_test_y_xts = cur_train_info$cur_test_y_xts
      cur_test_y_vec = cur_train_info$cur_test_y_vec
      cur_holiday_dummy_valid <-
        cur_train_info$cur_holiday_dummy_valid
      train_holiday_dummy <- cur_train_info$train_holiday_dummy
      test_holiday_dummy <- cur_train_info$test_holiday_dummy
      cur_update_hour_dummy_valid <-
        cur_train_info$cur_update_hour_dummy_valid
      train_update_hour_dummy <-
        cur_train_info$train_update_hour_dummy
      test_update_hour_dummy <-
        cur_train_info$test_update_hour_dummy
      lambda = cur_train_info$lambda
      cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
      cur_train_datetime = cur_train_info$cur_train_datetime
      cur_test_datetime = cur_train_info$cur_test_datetime
      has_weekly_data = cur_train_info$has_weekly_data
      
      bc_ts_ss  <-
        bsts::AddLocalLinearTrend(list(), cur_train_y_bc_ts)  # i bet the trend is linear most of time
      bc_ts_ss <- bsts::AddAutoAr(bc_ts_ss, cur_train_y_bc_ts)
      if (length(cur_train_y_bc_ts) > 7 * 24) {
        bc_ts_ss <-
          bsts::AddSeasonal(bc_ts_ss, cur_train_y_bc_ts, nseasons = 7 * 24)  # prefer this over additive model without interaction
      } else{
        bc_ts_ss <-
          bsts::AddSeasonal(bc_ts_ss, cur_train_y_bc_ts, nseasons = y_freq)
      }
      cur_fit_args_list <-
        c(cur_fit_args_list, list(
          list(
            formula = cur_train_y_bc_ts,
            state.specification = bc_ts_ss,
            niter = 500
          )
        ))
      cur_fcast_args_list <-
        c(cur_fcast_args_list, list(list(h = hold_out_h)))
    }
    
    ptime.start <- Sys.time()
    cv_res_list <- foreach (
      cur_fit_args = iter(cur_fit_args_list),
      cur_fcast_args = iter(cur_fcast_args_list),
      i = icount(),
      .packages = c("bsts", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      stopifnot(class(cur_train_y_ts) == "ts")
      args_list <- list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = bsts::bsts,
        fit_fun_arg_list = cur_fit_args,
        cur_predict_fun = bsts::predict.bsts,
        predict_fun_arg_list = cur_fcast_args
      )
      do.call(do.fit.predict,
              args = args_list)
    }
    # back transform to original ts
    for (train_h_i in seq_along(cur_train_h_seq)) {
      train_h <- cur_train_h_seq[[train_h_i]]
      cur_cv_res <- cv_res_list[[train_h_i]]
      lambda <- cur_lambda_list[[train_h_i]]
      cv_res_list[[train_h_i]]$cur.pred.values <-
        InvBoxCox(as.numeric(cur_cv_res$cur.pred.values$mean), lambda = lambda)
    }
    # collect cv result
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    
    
    # tends to over-forecast which make it inferior to bagged model when it comes to MAPE metric which prefer under-forecast over over-forecast
    cur.model.name <- "BoxCox GAM"
    cv_res_list <- NULL
    cur_fit_args_list <- NULL
    gam_fit_args_list <- NULL
    gam_fcast_args_list <- NULL
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_rng = cur_train_info$cur_train_rng
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_train_y_xts = cur_train_info$cur_train_y_xts
      cur_train_size = cur_train_info$cur_train_size
      cur_test_rng = cur_train_info$cur_test_rng
      cur_test_y_ts = cur_train_info$cur_test_y_ts
      cur_test_y_xts = cur_train_info$cur_test_y_xts
      cur_test_y_vec = cur_train_info$cur_test_y_vec
      cur_holiday_dummy_valid <-
        cur_train_info$cur_holiday_dummy_valid
      train_holiday_dummy <- cur_train_info$train_holiday_dummy
      test_holiday_dummy <- cur_train_info$test_holiday_dummy
      cur_update_hour_dummy_valid <-
        cur_train_info$cur_update_hour_dummy_valid
      train_update_hour_dummy <-
        cur_train_info$train_update_hour_dummy
      test_update_hour_dummy <-
        cur_train_info$test_update_hour_dummy
      lambda = cur_train_info$lambda
      cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
      cur_train_datetime = cur_train_info$cur_train_datetime
      cur_test_datetime = cur_train_info$cur_test_datetime
      has_weekly_data = cur_train_info$has_weekly_data
      
      cur_fit_args_list <- c(cur_fit_args_list,
                             list(
                               list(
                                 cur_bid_str = cur_bid_str,
                                 cur_pid_str = cur_pid_str,
                                 cur_train_y_ts = cur_train_y_bc_ts,
                                 cur.model.name = cur.model.name,
                                 hold_out_h = hold_out_h,
                                 lambda = lambda,
                                 has_weekly_data = has_weekly_data,
                                 cur_train_datetime = cur_train_datetime,
                                 cur_test_datetime = cur_test_datetime,
                                 cur_train_rng = cur_train_rng,
                                 cur_test_rng = cur_test_rng
                               )
                             ))
      
      
      cur_bc_decomp <- NULL
      tryCatch({
        cur_bc_decomp <-
          stl(cur_train_y_bc_ts,
              s.window = "periodic",
              robust = TRUE)
      }, error = function (e) {
        log4r::error(logger, as.character(e))
        print(head(sys.calls()))
      })
      
      gam_bc_mat <-
        data.frame(
          y = as.numeric(cur_train_y_bc_ts),
          # nWeek = as.integer(strftime(cur_train_datetime, "%W")),
          # nDOY = as.integer(strftime(cur_train_datetime, "%j")),
          hourofday = as.integer(format(cur_train_datetime, "%H")),
          tt = cur_hourly_vv_df$tt[cur_train_rng]
        )
      if (!is.null(cur_bc_decomp)) {
        gam_bc_mat <-
          as.data.frame(cbind(gam_bc_mat, seasadj_y = as.numeric(seasadj(
            cur_bc_decomp
          ))))
      }
      if (has_weekly_data) {
        gam_bc_mat <-
          as.data.frame(cbind(gam_bc_mat, dayofweek = as.integer(cur_train_datetime$wday)))
      }
      cur.using.holiday.dummy <- cur_holiday_dummy_valid
      if (cur.using.holiday.dummy) {
        gam_bc_mat$is_holiday = train_holiday_dummy
      }
      cur.using.update_hour.dummy <-
        using_update_hour && cur_update_hour_dummy_valid
      if (cur.using.update_hour.dummy) {
        gam_bc_mat$is_update_hour <- train_update_hour_dummy
      }
      cur_xreg_y_colnames <- c("y", "seasadj_y")
      cur_xreg <-
        gam_bc_mat[,!colnames(gam_bc_mat) %in% cur_xreg_y_colnames]
      fxreg_gam_mat <-
        data.frame(# nDOY = as.integer(strftime(cur_test_datetime, "%j")),
          hourofday = as.numeric(format(cur_test_datetime, "%H")),
          tt = cur_hourly_vv_df$tt[cur_test_rng])
      if (has_weekly_data) {
        fxreg_gam_mat <-
          as.data.frame(cbind(fxreg_gam_mat, dayofweek = as.numeric(cur_test_datetime$wday)))
      }
      if (cur.using.holiday.dummy) {
        fxreg_gam_mat$is_holiday = test_holiday_dummy
      }
      if (cur.using.update_hour.dummy) {
        fxreg_gam_mat$is_update_hour <- test_update_hour_dummy
      }
      cur_fxreg <- fxreg_gam_mat
      # remove constant columns and check for multicollinearity and remove correlated columns using VIF
      cur_sel_pred_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) != 0]
      cur_constant_col_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) == 0]
      cur.excluded.colnames <- NULL
      if (length(cur_sel_pred_names) < ncol(cur_xreg)) {
        cur_xreg <- subset(cur_xreg, select = cur_sel_pred_names)
        cur_fxreg <- subset(cur_fxreg, select = cur_sel_pred_names)
        cur.excluded.colnames <- cur_constant_col_names
      }
      if (is.rank.deficient(cur_xreg)) {
        cur.col.trim.res.list <- trim.col.vif(cur_xreg, cur_fxreg)
        cur_xreg <- cur.col.trim.res.list$xreg
        cur_fxreg <- cur.col.trim.res.list$fxreg
        cur.vif.excluded.colnames <-
          cur.col.trim.res.list$excluded_colnames
        cur.excluded.colnames <-
          union(cur.excluded.colnames, cur.vif.excluded.colnames)
        if ("is_holiday" %in% cur.excluded.colnames) {
          cur.using.holiday.dummy <- FALSE
        }
        if ("is_update_hour" %in% cur.excluded.colnames) {
          cur.using.update_hour.dummy <- FALSE
        }
      }
      stopifnot(colnames(cur_xreg) == colnames(cur_fxreg))
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.holiday"]] <-
        list(cur.using.holiday.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.update_hour"]] <-
        list(cur.using.update_hour.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["xreg"]] <-
        list(cur_xreg)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["fxreg"]] <-
        list(cur_fxreg)
      
      # cur_nWeek_k <-
      #   length(unique(gam_bc_mat[["nWeek"]])) - 1
      # cur_nDOY_k <- length(unique(gam_bc_mat[["nDOY"]])) - 1
      gam_bc_mat <-
        gam_bc_mat[, !colnames(gam_bc_mat) %in% cur.excluded.colnames, drop = FALSE]
      fxreg_gam_mat <-
        fxreg_gam_mat[,!colnames(fxreg_gam_mat) %in% cur.excluded.colnames, drop = FALSE]
      
      ctrl <-
        list(niterEM = 0,
             msVerbose = TRUE,
             optimMethod = "L-BFGS-B")
      cur.model.na <- FALSE
      cur.model.fit <- NULL
      cur.model.na <- FALSE
      fit.start.time <- Sys.time()
      cur.gam.formula.str <- gen.gam.formula.str(gam_bc_mat)
      cur.gam.formula <- as.formula(cur.gam.formula.str)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["formula"]] <-
        cur.gam.formula
      # tryCatch({
      #   # formula and data are function name hence it is tricky to pass a named argument list to mgcv::gam API
      #   if (has_weekly_data) {
      #     if (cur_holiday_dummy_valid) {
      #       cur.gam.formula.str <-
      #         "y ~ is_holiday + s(tt) + te(hourofday,dayofweek,k = c(cur_hourofday_k, cur_dayofweek_k),bs = c(\"cc\", \"cc\"))"
      #     } else{
      #       cur.gam.formula.str <-
      #         "y ~ s(tt) + te(hourofday,dayofweek,k = c(cur_hourofday_k, cur_dayofweek_k),bs = c(\"cc\", \"cc\"))"
      #     }
      #   }
      #   else {
      #     if (cur_holiday_dummy_valid) {
      #       # linear trend
      #       cur.gam.formula.str <-
      #         "y ~ is_holiday + tt + s(hourofday, k = cur_hourofday_k, bs = \"cc\")"
      #     } else {
      #       # linear trend
      #       cur.gam.formula.str <-
      #         "y ~ tt + s(hourofday, k = cur_hourofday_k, bs = \"cc\")"
      #     }
      #   }
      #   if (using_update_hour && cur_update_hour_dummy_valid) {
      #     cur.gam.formula.str <-
      #       paste(cur.gam.formula.str, "+ is_update_hour")
      #   }
      #   cur.gam.formula <- as.formula(cur.gam.formula.str)
      # },
      # error = function(e) {
      #   log4r::error(logger, as.character(e))
      #   cur.model.na <<- TRUE
      # })
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      log4r::info(
        logger,
        sprintf(
          "%d points, %s '%s', parameter preparation time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      
      gam_fit_args_list <-
        c(gam_fit_args_list, list(
          list(
            formula = cur.gam.formula,
            data = gam_bc_mat,
            ctrl = ctrl
          )
        ))
      gam_fcast_args_list <-
        c(gam_fcast_args_list, list(
          list(
            type = "response",
            h = hold_out_h,
            newdata = fxreg_gam_mat
          )
        ))
      
    } # for every CV size fitting arguments list
    # cur_args <- iter(cur_fit_args_list)
    ptime.start <- Sys.time()
    cv_res_list <- foreach (
      cur_args = iter(cur_fit_args_list),
      cur_gam_fit_args = iter(gam_fit_args_list),
      cur_gam_fcast_args = iter(gam_fcast_args_list),
      # cur_hourofday_k = cur_hourofday_k,  # still not works
      # cur_dayofweek_k = cur_dayofweek_k,
      i = icount(),
      # .combine = comb,
      .packages = c("forecast", "log4r", "iterators", "mgcv"),
      # .export = c("cur_hourofday_k", "cur_nWeek_k", "cur_dayofweek_k", "cur_nDOY_k"), # not working
      .verbose = TRUE
    ) %dopar% {
      # %dopar% : object 'cur_hourofday_k' not found or object 'gam_bc_mat' not found, probably caused by two level of indirection
      stopifnot(class(cur_args) == "list")
      log4r::info(
        logger,
        sprintf(
          "%d process. len:%d\t%d samples, %d train range sample count",
          i,
          length(cur_args),
          length(cur_args$cur_train_y_ts),
          length(cur_args$cur_train_rng)
          # paste(names(cur_args), collapse = " ")
        )
      )
      
      cur_real_args <- list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = mgcv::gam,
        fit_fun_arg_list = cur_gam_fit_args,
        cur_predict_fun = mgcv::predict.gam,
        predict_fun_arg_list = cur_gam_fcast_args
      )
      do.call(do.fit.predict, args = cur_real_args)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    # back transform to original ts
    for (train_h_i in seq_along(cur_train_h_seq)) {
      train_h <- cur_train_h_seq[[train_h_i]]
      cur_cv_res <- cv_res_list[[train_h_i]]
      lambda <- cur_lambda_list[[train_h_i]]
      cv_res_list[[train_h_i]]$cur.pred.values <-
        InvBoxCox(as.numeric(cur_cv_res$cur.pred.values), lambda = lambda)
    }
    # collect cv result
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    
    # # way much worse than the rest
    # cur.model.name <-
    #   "Box-Cox tslm"  # parallel version of tslm is much involed and no faster than single-core version
    # cur_fit_args_list <- NULL
    # cur_fcast_args_list <- NULL
    # for (train_h in cur_train_h_seq) {
    #   cur_train_h_str <- as.character(train_h)
    #   cur_train_info <-
    #     train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
    #   cur_train_y_ts = cur_train_info$cur_train_y_ts
    #   cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
    #   cur_train_size = length(cur_train_y_ts)
    #   lambda <- cur_train_info$lambda
    #   cur_train_four_reg <- cur_train_info$train_four_reg
    #   cur_test_four_reg <- cur_train_info$test_four_reg
    #   cur_train_bc_four_reg <- cur_train_info$train_bc_four_reg
    #   cur_test_bc_four_reg <- cur_train_info$test_bc_four_reg
    #   cur_train_rng <- cur_train_info$cur_train_rng
    #   cur_test_rng <- cur_train_info$cur_test_rng
    #   cur_train_tt <- cur_hourly_vv_df$tt[cur_train_rng]
    #   cur_test_tt <- cur_hourly_vv_df$tt[cur_test_rng]
    #   cur_holiday_dummy_valid <-
    #     cur_train_info$cur_holiday_dummy_valid
    #   train_holiday_dummy <- cur_train_info$train_holiday_dummy
    #   test_holiday_dummy <- cur_train_info$test_holiday_dummy
    #
    #   if (ncol(cur_train_bc_four_reg) + 1 >= cur_train_size) {
    #     warn(
    #       logger,
    #       sprintf(
    #         "%s '%s' unable to fit %d point with %d parameters",
    #         cur_out_comm_prefix,
    #         cur.model.name,
    #         cur_train_size,
    #         ncol(cur_train_bc_four_reg) + 1
    #       )
    #     )
    #     require("usdm")
    #     v2 <- vifstep(cur_train_bc_four_reg, th = 10)
    #     sel_colnames <- as.character(v2@results$Variables)
    #     excluded_colnames <- as.character(v2@excluded)
    #     cur_filtered_xreg <- train_bc_four_reg[, sel_colnames]
    #     if (length(excluded_colnames) > 0) {
    #       info(logger,
    #            sprintf(
    #              "\tpredictors '%s' is excluded",
    #              paste(excluded_colnames, collapse = ",")
    #            ))
    #     }
    #     cur_train_bc_four_reg <- cur_filtered_xreg
    #     cur_test_bc_four_reg <- cur_test_bc_four_reg[, sel_colnames]
    #   }
    #
    #   cur_four_col_names <-
    #     make.names(colnames(cur_train_bc_four_reg))  # replace "-" with "." to pass formula extraction
    #   cur_four_col_n <- length(cur_four_col_names)
    #   cur_data <-
    #     data.frame(cbind(cur_train_tt, cur_train_bc_four_reg))
    #   colnames(cur_data)[(ncol(cur_data) - cur_four_col_n + 1):ncol(cur_data)] <-
    #     cur_four_col_names
    #   cur_newx <-
    #     data.frame(cbind(cur_test_tt, cur_test_bc_four_reg))
    #   colnames(cur_newx)[(ncol(cur_newx) - cur_four_col_n + 1):ncol(cur_newx)] <-
    #     cur_four_col_names
    #
    #   cur_formula <- as.formula(paste(sprintf(
    #     "cur_train_bc_y_ts ~ cur_train_tt + %s",
    #     paste(cur_four_col_names, collapse = " + ")
    #   )))
    #   if (cur_holiday_dummy_valid) {
    #     cur_data <- cbind(cur_data, is_holiday = train_holiday_dummy)
    #     cur_newx <- cbind(cur_data, is_holiday = test_holiday_dummy)
    #     cur_formula <- as.formula(paste(
    #       sprintf(
    #         "cur_train_bc_y_ts ~ cur_train_tt + %s + is_holiday",
    #         paste(cur_four_col_names, collapse = " + ")
    #       )
    #     ))
    #   }
    #
    #   cur_fit_args_list <-
    #     c(cur_fit_args_list, list(list(
    #       formula = cur_formula,
    #       data = cur_data
    #     )))
    #   cur_fcast_args_list <-
    #     c(cur_fcast_args_list, list(list(h = hold_out_h, newx = cur_newx)))
    # }
    # cv_model_list <-
    #   foreach (
    #     cur_fit_args = iter(cur_fit_args_list),
    #     cur_train_bc_y_ts = iter(cur_train_y_bc_ts_list)  # export current train y ts
    #   ) %do% {
    #     cur_args <- c(
    #       cur_bid_str = cur_bid_str,
    #       cur_pid_str = cur_pid_str,
    #       cur.model.name = cur.model.name,
    #       cur_fit_fun = forecast::tslm,
    #       fit_fun_arg_list = list(cur_fit_args)
    #     )
    #     do.call(do.fit,
    #             args = cur_args)
    #   }
    #
    # cv_pred_list <- foreach(
    #   cur_fit_info = iter(cv_model_list),
    #   cur_fcast_args = iter(cur_fcast_args_list),
    #   cur_train_size = iter(cur_train_h_seq),
    #   # to print
    #   lambda = iter(cur_lambda_list)
    # ) %do% {
    #   cur.model.fit <- cur_fit_info$cur.model.fit
    #   cur.model.df <- model.df(cur.model.fit)
    #   # cur.model.cv <- CV(cur.model.fit)
    #   cur.lm.coef.p <-
    #     summary(cur.model.fit)$coefficients[, 4] # https://stackoverflow.com/questions/5587676/pull-out-p-values-and-r-squared-from-a-linear-regression#5588638
    #   insig_pred_var_names <-
    #     names(cur.lm.coef.p[which(cur.lm.coef.p >= 0.05)])
    #   if (length(insig_pred_var_names)) {
    #     warn(
    #       logger,
    #       sprintf(
    #         "%s train sample size %d,  '%s' predictors %d/%d(%.2f%%) '%s' is statistically insignificant",
    #         cur_out_comm_prefix,
    #         cur_train_size,
    #         cur.model.name,
    #         length(insig_pred_var_names),
    #         length(coef(cur.model.fit)),
    #         length(insig_pred_var_names) * 1.0 / length(coef(cur.model.fit)) * 100,
    #         paste(insig_pred_var_names, collapse = ",")
    #       )
    #     )
    #   }
    #   # Error in forecast.lm(tslm.fit, h = hold_out_h) :
    #   # Variables not found in newdata
    #   cur.fcast <-
    #     do.call(stats::predict, args = c(list(cur.model.fit), cur_fcast_args))
    #   forecast::InvBoxCox(cur.fcast[1:hold_out_h], lambda = lambda)
    # }
    # for (train_h_i in seq_along(cur_train_h_seq)) {
    #   train_h <- cur_train_h_seq[[train_h_i]]
    #   cur_cv_res <- cv_model_list[[train_h_i]]
    #   cur.model.fit <- cur_cv_res$cur.model.fit
    #   cur.fit.time.taken <- cur_cv_res$fit.time.taken
    #   cur.pred.values <- cv_pred_list[[train_h_i]]
    #   cur_train_h_str <-
    #     as.character(train_h)
    #   # no elegent way to pass by reference
    #   model_fit_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
    #     c(model_fit_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
    #       list(cur.model.fit))
    #   model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
    #     c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
    #       list(as.numeric(cur.fit.time.taken, units = "secs")))
    #   forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
    #     c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]],
    #       list(cur.pred.values[1:hold_out_h]))
    # }
    
    
    cur_fit_args_list <- NULL
    cur_fcast_args_list <- NULL
    cur.model.name <- "BoxCox auto.arima with fourier terms"
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      lambda <- cur_train_info$lambda
      cur_train_bc_four_reg <- cur_train_info$train_bc_four_reg
      cur_test_bc_four_reg <- cur_train_info$test_bc_four_reg
      cur_holiday_dummy_valid <-
        cur_train_info$cur_holiday_dummy_valid
      train_holiday_dummy <- cur_train_info$train_holiday_dummy
      test_holiday_dummy <- cur_train_info$test_holiday_dummy
      cur_xreg <- cur_train_bc_four_reg
      cur_fxreg <- cur_test_bc_four_reg
      cur_update_hour_dummy_valid <-
        cur_train_info$cur_update_hour_dummy_valid
      train_update_hour_dummy <-
        cur_train_info$train_update_hour_dummy
      test_update_hour_dummy <-
        cur_train_info$test_update_hour_dummy
      cur.using.holiday.dummy <- cur_holiday_dummy_valid
      if (cur.using.holiday.dummy) {
        cur_xreg <- cbind(cur_xreg, is_holiday = train_holiday_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_holiday = test_holiday_dummy)
      }
      cur.using.update_hour.dummy <-
        using_update_hour && cur_update_hour_dummy_valid
      if (cur.using.update_hour.dummy) {
        cur_xreg <-
          cbind(cur_xreg, is_update_hour = train_update_hour_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_update_hour = test_update_hour_dummy)
      }
      # remove constant value predictors
      cur_sel_pred_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) != 0]
      cur_constant_col_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) == 0]
      cur.excluded.colnames <- NULL
      if (length(cur_sel_pred_names) < ncol(cur_xreg)) {
        cur_xreg <- subset(cur_xreg, select = cur_sel_pred_names)
        cur_fxreg <- subset(cur_fxreg, select = cur_sel_pred_names)
        cur.excluded.colnames <- cur_constant_col_names
      }
      
      if (is.rank.deficient(cur_xreg)) {
        cur.col.trim.res.list <- trim.col.vif(cur_xreg, cur_fxreg)
        cur_xreg <- cur.col.trim.res.list$xreg
        cur_fxreg <- cur.col.trim.res.list$fxreg
        cur.vif.excluded.colnames <-
          cur.col.trim.res.list$excluded_colnames
        cur.excluded.colnames <-
          union(cur.excluded.colnames, cur.vif.excluded.colnames)
        if ("is_holiday" %in% cur.excluded.colnames) {
          cur.using.holiday.dummy <- FALSE
        }
        if ("is_update_hour" %in% cur.excluded.colnames) {
          cur.using.update_hour.dummy <- FALSE
        }
      }
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.holiday"]] <-
        list(cur.using.holiday.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.update_hour"]] <-
        list(cur.using.update_hour.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["xreg"]] <-
        list(cur_xreg)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["fxreg"]] <-
        list(cur_fxreg)
      cur_fit_args_list <- c(cur_fit_args_list,
                             list(
                               list(
                                 y = cur_train_y_ts,
                                 lambda = lambda,
                                 xreg = cur_xreg,
                                 seasonal = FALSE,
                                 stepwise = FALSE
                               )
                             ))
      cur_fcast_args_list <- c(cur_fcast_args_list,
                               list(list(
                                 h = hold_out_h,
                                 lambda = lambda,
                                 xreg = cur_fxreg
                               )))
    }
    ptime.start <- Sys.time()
    cv_res_list <- foreach(
      cur_train_y_ts = iter(cur_train_y_ts_list),
      cur_fit_args = iter(cur_fit_args_list),
      cur_fcast_args = iter(cur_fcast_args_list),
      i = icount(),
      .packages = c("forecast", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      args_list = list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = forecast::auto.arima,
        fit_fun_arg_list = cur_fit_args,
        cur_predict_fun = forecast::forecast,
        predict_fun_arg_list = cur_fcast_args
      )
      do.call(do.fit.predict, args = args_list)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    # collect results
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    
    
    cur_fit_args_list <- NULL
    cur.model.name <- "TBATS"
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_mul_seas_train_ts <-
        msts(cur_train_y_ts, seasonal.periods = c(24, 7 * 24))
      cur_fit_args_list <- c(cur_fit_args_list,
                             list(list(y = cur_mul_seas_train_ts)))
    }
    ptime.start <- Sys.time()
    cv_res_list <- foreach(
      cur_train_y_ts = iter(cur_train_y_ts_list),
      cur_args = iter(cur_fit_args_list),
      i = icount(),
      .packages = c("forecast", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      args_list = list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = forecast::tbats,
        fit_fun_arg_list = cur_args,
        cur_predict_fun = forecast::forecast,
        predict_fun_arg_list = c(h = hold_out_h)
      )
      do.call(do.fit.predict, args = args_list)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    # collect results
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    
    
    cur.model.name <- "BoxCox NNAR"
    cur_fit_args_list <- NULL
    cur_fcast_args_list <- NULL
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_train_y_bc_ts = cur_train_info$cur_train_y_bc_ts
      cur_test_y_bc_ts = cur_train_info$cur_test_y_bc_ts
      lambda <-
        cur_train_info$lambda
      cur_holiday_dummy_valid <-
        cur_train_info$cur_holiday_dummy_valid
      train_holiday_dummy <- cur_train_info$train_holiday_dummy
      test_holiday_dummy <- cur_train_info$test_holiday_dummy
      cur_update_hour_dummy_valid <-
        cur_train_info$cur_update_hour_dummy_valid
      train_update_hour_dummy <-
        cur_train_info$train_update_hour_dummy
      test_update_hour_dummy <-
        cur_train_info$test_update_hour_dummy
      cur_fit_args <- list(y = cur_train_y_ts, lambda = lambda)
      cur_fcast_args <- list(h = hold_out_h, lambda = lambda)
      cur_xreg <- NULL
      cur_fxreg <- NULL
      cur.using.holiday.dummy <- cur_holiday_dummy_valid
      if (cur.using.holiday.dummy) {
        cur_xreg <- cbind(cur_xreg, is_holiday = train_holiday_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_holiday = test_holiday_dummy)
      }
      cur.using.update_hour.dummy <-
        using_update_hour && cur_update_hour_dummy_valid
      if (cur.using.update_hour.dummy) {
        cur_xreg <-
          cbind(cur_xreg, is_update_hour = train_update_hour_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_update_hour = test_update_hour_dummy)
      }
      cur_sel_pred_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) != 0]
      cur_constant_col_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) == 0]
      cur.excluded.colnames <- NULL
      if (length(cur_sel_pred_names) < ncol(cur_xreg)) {
        cur_xreg <- subset(cur_xreg, select = cur_sel_pred_names)
        cur_fxreg <- subset(cur_fxreg, select = cur_sel_pred_names)
        cur.excluded.colnames <- cur_constant_col_names
      }
      stopifnot(nrow(cur_xreg) == nrow(cur_train_y_bc_ts))
      stopifnot(nrow(cur_fxreg) == nrow(cur_test_y_bc_ts))
      if (is.rank.deficient(cur_xreg)) {
        cur.col.trim.res.list <- trim.col.vif(cur_xreg, cur_fxreg)
        cur_xreg <- cur.col.trim.res.list$xreg
        cur_fxreg <- cur.col.trim.res.list$fxreg
        cur.vif.excluded.colnames <-
          cur.col.trim.res.list$excluded_colnames
        cur.excluded.colnames <-
          union(cur.excluded.colnames, cur.vif.excluded.colnames)
        if ("is_holiday" %in% cur.excluded.colnames) {
          cur.using.holiday.dummy <- FALSE
        }
        if ("is_update_hour" %in% cur.excluded.colnames) {
          cur.using.update_hour.dummy <- FALSE
        }
      }
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.holiday"]] <-
        list(cur.using.holiday.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.update_hour"]] <-
        list(cur.using.update_hour.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["xreg"]] <-
        list(cur_xreg)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["fxreg"]] <-
        list(cur_fxreg)
      cur_fit_args <-
        c(cur_fit_args, list(xreg = cur_xreg))
      cur_fcast_args <-
        c(cur_fcast_args, list(xreg = cur_fxreg))
      cur_fit_args_list <- c(cur_fit_args_list,
                             list(cur_fit_args))
      cur_fcast_args_list <- c(cur_fcast_args_list,
                               list(cur_fcast_args))
    }
    
    ptime.start <- Sys.time()
    cv_res_list <- foreach(
      cur_train_y_ts = iter(cur_train_y_ts_list),
      cur_fit_args = iter(cur_fit_args_list),
      cur_fcast_args = iter(cur_fcast_args_list),
      i = icount(),
      .packages = c("forecast", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      args_list = list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = forecast::nnetar,
        fit_fun_arg_list = cur_fit_args,
        cur_predict_fun = forecast::forecast,
        predict_fun_arg_list = cur_fcast_args
      )
      do.call(do.fit.predict, args = args_list)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    # collect results
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    
    
    cur.model.name <- "NNAR"
    cur_fit_args_list <- NULL
    cur_fcast_args_list <- NULL
    for (train_h in cur_train_h_seq) {
      cur_train_h_str <- as.character(train_h)
      cur_train_info <-
        train_data_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]]
      cur_train_y_ts = cur_train_info$cur_train_y_ts
      cur_test_y_ts = cur_train_info$cur_test_y_ts
      cur_holiday_dummy_valid <-
        cur_train_info$cur_holiday_dummy_valid
      train_holiday_dummy <- cur_train_info$train_holiday_dummy
      test_holiday_dummy <- cur_train_info$test_holiday_dummy
      cur_update_hour_dummy_valid <-
        cur_train_info$cur_update_hour_dummy_valid
      train_update_hour_dummy <-
        cur_train_info$train_update_hour_dummy
      test_update_hour_dummy <-
        cur_train_info$test_update_hour_dummy
      cur_fit_args <- list(y = cur_train_y_ts)
      cur_fcast_args <- list(h = hold_out_h)
      cur_xreg <- NULL
      cur_fxreg <- NULL
      cur.using.holiday.dummy <- cur_holiday_dummy_valid
      if (cur.using.holiday.dummy) {
        cur_xreg <- cbind(cur_xreg, is_holiday = train_holiday_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_holiday = test_holiday_dummy)
      }
      cur.using.update_hour.dummy <-
        using_update_hour && cur_update_hour_dummy_valid
      if (cur.using.update_hour.dummy) {
        cur_xreg <-
          cbind(cur_xreg, is_update_hour = train_update_hour_dummy)
        cur_fxreg <-
          cbind(cur_fxreg, is_update_hour = test_update_hour_dummy)
      }
      cur_sel_pred_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) != 0]
      cur_constant_col_names <-
        colnames(cur_xreg)[apply(cur_xreg, 2, var, na.rm = TRUE) == 0]
      cur.excluded.colnames <- NULL
      if (length(cur_sel_pred_names) < ncol(cur_xreg)) {
        cur_xreg <- subset(cur_xreg, select = cur_sel_pred_names)
        cur_fxreg <- subset(cur_fxreg, select = cur_sel_pred_names)
        cur.excluded.colnames <- cur_constant_col_names
      }
      stopifnot(nrow(cur_xreg) == nrow(cur_train_y_ts))
      stopifnot(nrow(cur_fxreg) == nrow(cur_test_y_ts))
      if (is.rank.deficient(cur_xreg)) {
        cur.col.trim.res.list <- trim.col.vif(cur_xreg, cur_fxreg)
        cur_xreg <- cur.col.trim.res.list$xreg
        cur_fxreg <- cur.col.trim.res.list$fxreg
        cur.vif.excluded.colnames <-
          cur.col.trim.res.list$excluded_colnames
        cur.excluded.colnames <-
          union(cur.excluded.colnames, cur.vif.excluded.colnames)
        if ("is_holiday" %in% cur.excluded.colnames) {
          cur.using.holiday.dummy <- FALSE
        }
        if ("is_update_hour" %in% cur.excluded.colnames) {
          cur.using.update_hour.dummy <- FALSE
        }
      }
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.holiday"]] <-
        list(cur.using.holiday.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["using.update_hour"]] <-
        list(cur.using.update_hour.dummy)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["xreg"]] <-
        list(cur_xreg)
      xreg_info_list[[cur_bid_str]][[cur_pid_str]][[cur.model.name]][[cur_train_h_str]][["fxreg"]] <-
        list(cur_fxreg)
      cur_fit_args <-
        c(cur_fit_args, list(xreg = cur_xreg))
      cur_fcast_args <-
        c(cur_fcast_args, list(xreg = cur_fxreg))
      cur_fit_args_list <- c(cur_fit_args_list,
                             list(cur_fit_args))
      cur_fcast_args_list <- c(cur_fcast_args_list,
                               list(cur_fcast_args))
    }
    
    ptime.start <- Sys.time()
    cv_res_list <- foreach(
      cur_fit_args = iter(cur_fit_args_list),
      cur_fcast_args = iter(cur_fcast_args_list),
      i = icount(),
      .packages = c("forecast", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      args_list = list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = forecast::nnetar,
        fit_fun_arg_list = cur_fit_args,
        cur_predict_fun = forecast::forecast,
        predict_fun_arg_list = cur_fcast_args
      )
      do.call(do.fit.predict, args = args_list)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    # collect results
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    
    
    cur.model.name <- "baggedETS"
    ptime.start <- Sys.time()
    cv_res_list <- foreach (
      cur_train_y_ts = iter(cur_train_y_ts_list),
      i = icount(),
      # .combine = comb,
      .packages = c("forecast", "log4r"),
      .verbose = TRUE
    ) %dopar% {
      stopifnot(class(cur_train_y_ts) == "ts")
      args_list <- list(
        cur_bid_str = cur_bid_str,
        cur_pid_str = cur_pid_str,
        cur.model.name = cur.model.name,
        cur_fit_fun = forecast::baggedETS,
        fit_fun_arg_list = list(y = cur_train_y_ts),
        cur_predict_fun = forecast::forecast,
        predict_fun_arg_list = list(h = hold_out_h)
      )
      cur_train_h_str <-
        as.character(length(cur_train_y_ts))
      
      do.call(do.fit.predict,
              args = args_list)
    }
    ptime.end <- Sys.time()
    ptime.taken <- ptime.end - ptime.start
    info(
      logger,
      sprintf(
        "%s '%s' %d fold CV time taken: %s",
        cur_out_comm_prefix,
        cur.model.name,
        cur_CV_cnt,
        format(ptime.taken)
      )
    )
    # collect results
    collect.cv.res(
      cur_train_h_seq = cur_train_h_seq,
      cv_res_list = cv_res_list,
      model_fit_list = model_fit_list,
      model_fit_time_list = model_fit_time_list,
      forecast_list = forecast_list,
      h = hold_out_h
    )
    stopCluster(cl)
    
    
    subplot_cnt <- length(names(forecast_list))
    # graphics.off()
    # par("mar")
    # par(mar=c(1,1,1,1))
    # png("test_par.png",
    #     width = 1920,
    #     height = 720 * sub_plot_num)
    ggplot_list <- c()
    facet_ggplot_list <- c()
    hourly_pred_plot_mat <- NULL
    # op <- par(mfrow = c(subplot_cnt, 1))
    for (min_train_h in names(forecast_list[[cur_bid_str]][[cur_pid_str]])) {
      cur_plot_title <- ""
      cur_plot_main_title <- ""
      cur_plot_sub_title <- ""
      cur_pred_plot_mat <- NULL
      for (model.name in names(forecast_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]])) {
        cur_actual_y_ts <-
          test_ts_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]]
        cur_actual_y_xts <-
          test_xts_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]]
        cur_forecast_vec <-
          as.numeric(unlist(forecast_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]]))
        cur_forecast <-
          ts(
            cur_forecast_vec,
            start = start(cur_actual_y_ts),
            frequency = y_freq
          )
        cur_pred_plot_mat <-
          rbind(cur_pred_plot_mat,
                data.frame(
                  x = index(cur_actual_y_xts),
                  y = as.numeric(cur_forecast),
                  model.name = rep(model.name, length(cur_forecast))
                ))
        cur_rmse <- rmse(cur_actual_y_ts, cur_forecast)
        rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]] <-
          c(rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]], list(cur_rmse))
        cur_mape <- mape(cur_actual_y_ts, cur_forecast)
        mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]] <-
          c(mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]], list(cur_mape))
        cur_max_train_size <-
          as.integer(min_train_h) + hold_out_h - 1
        # cur_plot_main_title <-
        #   sprintf("train size = %s-%d",
        #           min_train_h,
        #           cur_max_train_size)
        cur_plot_main_title <-
          sprintf("train size = %s/%ddays",
                  min_train_h,
                  ceiling(as.integer(min_train_h) / 24))
        cur_model_plot_sub_title <-
          sprintf("'%s' RMSE = %.4f, MAPE = %.4f",
                  model.name,
                  cur_rmse,
                  cur_mape)
        cur_plot_title <-
          paste(
            cur_plot_main_title,
            cur_model_plot_sub_title,
            collapse = "\t",
            sep = "\n"
          )
        # info(logger, cur_plot_title)
        cur_plot_sub_title <-
          paste(cur_plot_sub_title, cur_model_plot_sub_title, sep = "\n")
        
        # lines(cur_forecast,
        #       main = cur_plot_title,
        #       col = plot_color_list[[model.name]])
        
      } # for every model
      info(logger,
           paste(cur_plot_main_title, cur_plot_sub_title, collapse = "\n"))
      cur_pred_plot_mat <-
        rbind(cur_pred_plot_mat,
              data.frame(
                x = index(cur_actual_y_xts),
                y = as.numeric(cur_actual_y_ts),
                model.name = rep("Actual VV", length(cur_actual_y_ts))
              ))
      hourly_pred_plot_mat <-
        rbind(hourly_pred_plot_mat, cur_pred_plot_mat)
      require("scales")
      cur_gg_p <-
        ggplot(data = cur_pred_plot_mat, aes(x = x,
                                             y = y,
                                             color = model.name)) +
        geom_line(aes(linetype = model.name)) +
        geom_point() + # geom_point(aes(shape = model.name)) +
        scale_x_datetime(
          labels = date_format("%Y-%m-%d %H:%M"),
          breaks = date_breaks("4 hour"),
          expand = c(0, 0)
        ) +
        theme(axis.text.x = element_text(angle = 60,
                                         # vjust = 0.5,
                                         hjust = 1)) +
        xlab("Datetime") + ylab("Hourly Video Visit") + labs(title = cur_plot_main_title, subtitle = cur_plot_sub_title)
      ggplot_list <- c(ggplot_list, list(cur_gg_p))
      # plot_cols <- 1:ncol(cur_pred_plot_mat)
      # matplot(
      #   cur_pred_plot_mat,
      #   type = c("b", "c"),
      #   pch = 1,
      #   col = plot_cols,
      #   main = cur_plot_title,
      #   cex = 1.5
      # )
      # legend(
      #   "topright",
      #   legend = c(names(forecast_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]]), "actual"),
      #   col = plot_cols,
      #   pch = 1,
      #   cex = 1.5
      # )
      
      
      cur.pred.model.names <-
        unique(cur_pred_plot_mat[which(cur_pred_plot_mat$model.name != "Actual VV"), ]$model.name)
      cur_ref_vv_df <-
        subset(cur_pred_plot_mat, model.name == "Actual VV")
      cur_ref_vv_plot_df <-
        data.frame(
          x = cur_ref_vv_df$x,
          y = cur_ref_vv_df$y,
          model.name = rep(cur.pred.model.names, each = hold_out_h)
        )
      cur.pred.plot.df <-
        subset(cur_pred_plot_mat, model.name != "Actual VV")
      cur_gg_facet_p <-
        ggplot(data = cur.pred.plot.df, aes(x = x, y = y), col = cols) +
        geom_line(linetype = "dashed") +
        geom_point() +
        facet_grid(model.name ~ ., scales = "free") +
        geom_line(data = cur_ref_vv_plot_df, color = "magenta") +
        geom_point(data = cur_ref_vv_plot_df, color = "magenta") +
        scale_x_datetime(
          labels = date_format("%Y-%m-%d %H:%M"),
          breaks = date_breaks("4 hour"),
          expand = c(0, 0)
        ) +  theme(axis.text.x = element_text(angle = 60,
                                              # vjust = 0.5,
                                              hjust = 1)) +
        
        ylab("Hourly Video Visit") + xlab("Datetime")
      facet_ggplot_list <-
        c(facet_ggplot_list, list(cur_gg_facet_p))
    } # for every train size
    library(gridExtra)
    n <- length(ggplot_list)
    nCol <- floor(sqrt(n))
    nRow <- ceiling(n / nCol)
    do.call("grid.arrange", c(ggplot_list, ncol = nCol))  # https://stackoverflow.com/questions/10706753/how-do-i-arrange-a-variable-list-of-plots-using-grid-arrange#10706828
    # grid.arrange(unlist(ggplot_list), nrow = subplot_cnt, top = "model prediction comparsion")
    g <- do.call("arrangeGrob",
                 c(ggplot_list, ncol = nCol)) #generates g
    ggsave(
      sprintf(
        "%s/%s_model_pred_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      g,
      # https://stackoverflow.com/questions/17059099/saving-grid-arrange-plot-to-file#28136155
      width = 6 * nCol,
      height = 6 * nRow
    )
    n <- length(facet_ggplot_list)
    nCol <- floor(sqrt(n))
    nRow <- ceiling(n / nCol)
    do.call("grid.arrange", c(facet_ggplot_list, ncol = nCol))  # https://stackoverflow.com/questions/10706753/how-do-i-arrange-a-variable-list-of-plots-using-grid-arrange#10706828
    # grid.arrange(unlist(facet_ggplot_list), nrow = subplot_cnt, top = "model prediction comparsion")
    g <- do.call("arrangeGrob",
                 c(facet_ggplot_list, ncol = nCol)) #generates g
    ggsave(
      sprintf(
        "%s/%s_model_pred_facet_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      g,
      # https://stackoverflow.com/questions/17059099/saving-grid-arrange-plot-to-file#28136155
      width = 6 * nCol,
      height = 6 * nRow
    )
    
    
    require("animation")
    saveGIF({
      for (p in ggplot_list)
        print(p)
    }, movie.name = sprintf("%s/%s_pred.gif", gif_file_dir, out_file_desc_prefix), interval = 2, ani.width = 1920, ani.height = 1080)
    
    # par(op)
    # dev.off()
    
    ggplot(data = subset(hourly_pred_plot_mat, !is.na(hourly_pred_plot_mat$y)),
           aes(x = x, y = y, color = model.name)) +
      geom_line(aes(linetype = model.name)) +
      geom_point() + # geom_point(aes(shape = model.name)) +
      scale_x_datetime(
        labels = date_format("%Y-%m-%d %H:%M"),
        breaks = date_breaks("6 hour"),
        expand = c(0, 0)
      ) +  theme(axis.text.x = element_text(angle = 60,
                                            # vjust = 0.5,
                                            hjust = 1)) +
      xlab("Datetime") + ylab("Hourly Video Visit") + labs(
        title = sprintf(
          "%s '%s' Leave-one-day-out Rolling Prediction Comparsion",
          cur_bid_name,
          cur_pid_name
        )
      )
    ggsave(
      sprintf(
        "%s/%s_rolling_pred_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      width = 18,
      height = 6
    )
    
    model_hourly_pred_plot_mat <-
      subset(hourly_pred_plot_mat, model.name != "Actual VV")
    hourly.pred.model.names <-
      unique(hourly_pred_plot_mat[which(hourly_pred_plot_mat$model.name != "Actual VV"),]$model.name)
    model_num <- length(hourly.pred.model.names)
    hourly_vv_colors <- gg_color_hue(model_num + 1)
    hourly_vv_color_num <- length(hourly_vv_colors)
    model_hourly_pred_plot_mat$color <-
      rep(hourly_vv_colors[1:model_num], each = y_freq)
    hourly_ref_vv_df <-
      subset(hourly_pred_plot_mat, model.name == "Actual VV")
    hourly_ref_vv_plot_df <-
      data.frame(
        x = hourly_ref_vv_df$x,
        y = hourly_ref_vv_df$y,
        model.name = rep(hourly.pred.model.names, each = length(hourly_ref_vv_df$x))
      )
    ggplot(data = model_hourly_pred_plot_mat, aes(x = x,
                                                  y = y)) +
      facet_grid(model.name ~ ., scales = "free") +
      geom_point() +
      geom_line() +
      geom_line(data = hourly_ref_vv_plot_df, aes(x = x,
                                                  y = y,
                                                  color = "magenta")) +
      geom_point(data = hourly_ref_vv_plot_df, aes(x = x, y = y), color = "magenta") +
      # scale_color_manual(values = as.character(hourly_vv_colors)) +
      scale_color_discrete(name = "model.name", labels = c("actual")) +
      scale_x_datetime(breaks = date_breaks("4 hour"),
                       labels = date_format("%Y-%m-%d %H:%M")) +
      theme(axis.text.x = element_text(angle = 60,
                                       hjust = 1)) +
      xlab("datetime") +
      ylab("Hourly Video Visit") +
      ggtitle(
        sprintf(
          "%s '%s' Leave-one-day-out Rolling Prediction Comparsion",
          cur_bid_name,
          cur_pid_name
        )
      )
    ggsave(
      sprintf(
        "%s/%s_rolling_pred_facet_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      width = 18,
      height = 6
    )
    
    # plot daily VV prediction by aggregation
    daily_pred_plot_mat <-
      aggregate(
        hourly_pred_plot_mat$y,
        by = list(
          x = as.Date(as.character(hourly_pred_plot_mat$x)),
          # as.Date("2017-02-07 00:00:00 CST") == "2017-02-06"!!!!
          model.name = hourly_pred_plot_mat$model.name
        ),
        FUN = sum,
        na.rm = TRUE
      )
    grp_colnames <- colnames(daily_pred_plot_mat)
    colnames(daily_pred_plot_mat)[length(grp_colnames)] <- "y"
    ggplot(data = daily_pred_plot_mat,
           aes(x = x,
               y = y,
               color = model.name)) +
      geom_line(aes(linetype = model.name)) +
      geom_point() + # geom_point(aes(shape = model.name)) +
      scale_x_date(
        labels = date_format("%Y-%m-%d"),
        breaks = date_breaks("1 day"),
        expand = c(0, 0)
      ) +  theme(axis.text.x = element_text(angle = 60,
                                            # vjust = 0.5,
                                            hjust = 1)) +
      xlab("Date") + ylab("Daily Video Visit") + labs(
        title = sprintf(
          "%s '%s' Leave-one-day-out Rolling Prediction Comparsion",
          cur_bid_name,
          cur_pid_name
        )
      )
    ggsave(
      sprintf(
        "%s/%s_daily_rolling_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      width = 12,
      height = 6
    )
    # facet version forecast comparsion
    daily_ref_vv_df <-
      subset(daily_pred_plot_mat, model.name == "Actual VV")
    daily.pred.model.names <-
      unique(daily_pred_plot_mat[which(daily_pred_plot_mat$model.name != "Actual VV"),]$model.name)
    daily_ref_vv_plot_df <-
      data.frame(
        x = daily_ref_vv_df$x,
        y = daily_ref_vv_df$y,
        model.name = rep(daily.pred.model.names, each = length(daily_ref_vv_df$x))
      )
    daily.pred.plot.df <-
      subset(daily_pred_plot_mat, model.name != "Actual VV")
    daily.pred.y.ranges <-
      sapply(daily.pred.model.names, function(m.name) {
        cur_range <- list(range(range(
          subset(daily.pred.plot.df, model.name == m.name)$y
        ), range(daily_ref_vv_df$y)))
        cur_range[[1]][[1]] <-
          ifelse(cur_range[[1]][[1]] > 0, 0.9 * cur_range[[1]][[1]], 1.1 * cur_range[[1]][[1]]) # minimum of ylim
        cur_range[[1]][[2]] <-
          ifelse(cur_range[[1]][[2]] > 0, 1.1 * cur_range[[1]][[2]], cur_range[[1]][[2]] * 0.9) # maximum of ylim
        names(cur_range) <- as.character(m.name)
        cur_range
      })
    daily_y_range_dummy <-
      daily_ref_vv_plot_df  # replicate dimensons
    daily_y_range_dummy$y.ranges <- daily.pred.y.ranges
    ggplot(data = daily.pred.plot.df, aes(x = x, y = y), col = cols) +
      geom_line(linetype = "dashed") +
      geom_point() +
      # Error in (function (..., row.names = NULL, check.rows = FALSE, check.names = TRUE,  :
      # arguments imply differing number of rows: 91, 2
      # geom_blank(data = daily_y_range_dummy, aes(x = x, y = y.ranges)) +  # https://stackoverflow.com/questions/30280499/different-y-limits-on-ggplot-facet-grid-bar-graph#30281595
      facet_grid(model.name ~ ., scales = "free") +
      geom_line(data = daily_ref_vv_plot_df, color = "magenta") +
      geom_point(data = daily_ref_vv_plot_df, color = "magenta") +
      scale_x_date(
        labels = date_format("%Y-%m-%d"),
        breaks = date_breaks("1 day"),
        expand = c(0, 0)
      ) +  theme(axis.text.x = element_text(angle = 60,
                                            # vjust = 0.5,
                                            hjust = 1)) +
      xlab("Date") + ylab("Daily Video Visit") + labs(
        title = sprintf(
          "%s '%s' Leave-one-day-out Rolling Prediction Comparsion",
          cur_bid_name,
          cur_pid_name
        )
      )
    ggsave(
      sprintf(
        "%s/%s_daily_rolling_facet_comp.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      width = 12,
      height = 6
    )
    
    # plot train size versus error metrics
    model_mape_list <- c()
    for (min_train_h in names(mape_list[[cur_bid_str]][[cur_pid_str]])) {
      for (model.name in names(mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]])) {
        # cat differenct CV size stat value
        model_mape_list[[model.name]] <-
          c(model_mape_list[[model.name]], mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]])
      }
    }
    plot_x <-
      as.integer(names(mape_list[[cur_bid_str]][[cur_pid_str]]))
    mape_plot_df <- data.frame()
    cur_df <- NULL
    for (model.name in names(model_mape_list)) {
      cur_df <-
        data.frame(
          x = plot_x,
          y = as.numeric(model_mape_list[[model.name]]),
          model.name = rep(model.name, length(plot_x))
        )
      mape_plot_df <- rbind(mape_plot_df, cur_df)
    }
    
    model_rmse_list <- c()
    for (min_train_h in names(rmse_list[[cur_bid_str]][[cur_pid_str]])) {
      for (model.name in names(rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]])) {
        model_rmse_list[[model.name]] <-
          c(model_rmse_list[[model.name]], rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]])
      }
    }
    plot_x <-
      as.integer(names(rmse_list[[cur_bid_str]][[cur_pid_str]]))
    rmse_plot_df <- data.frame()
    cur_df <- NULL
    for (model.name in names(model_rmse_list)) {
      cur_df <-
        data.frame(
          x = plot_x,
          y = as.numeric(model_rmse_list[[model.name]]),
          model.name = rep(model.name, length(plot_x))
        )
      rmse_plot_df <- rbind(rmse_plot_df, cur_df)
    }
    require("grid")
    require("gridExtra")
    ggplot_err_p_list <- c()
    mape_p <-
      ggplot(data = subset(mape_plot_df,!is.na(y)),
             aes(x = x,
                 y = y,
                 color = model.name)) + geom_line(aes(linetype = model.name)) +
      geom_point() + # geom_point(aes(color = model.name, shape = model.name)) +
      xlab("Train Sample Size") + ylab("MAPE Error")
    # lines(test_y_ts, col = "red")
    ggplot_err_p_list <- c(ggplot_err_p_list, list(mape_p))
    
    rmse_p <-
      ggplot(data = subset(rmse_plot_df, !is.na(y)),
             aes(x = x,
                 y = y,
                 color = model.name)) + geom_line(aes(linetype = model.name)) +
      geom_point() + # geom_point(aes(color = model.name, shape = model.name)) +
      xlab("Train Sample Size") + ylab("RMSE Error")
    ggplot_err_p_list <- c(ggplot_err_p_list, list(rmse_p))
    n <- length(ggplot_err_p_list)
    do.call("grid.arrange",
            c(
              ggplot_err_p_list,
              nrow = length(ggplot_err_p_list),
              top = sprintf(
                "%s Train Sample Size VS Leave-one-out Prediction Error Metrics",
                cur_out_comm_prefix
              )
            ))  # https://stackoverflow.com/questions/10706753/how-do-i-arrange-a-variable-list-of-plots-using-grid-arrange#10706828
    # grid.arrange(unlist(ggplot_err_p_list), nrow = subplot_cnt, top = "model prediction comparsion")
    g <- do.call("arrangeGrob",
                 c(ggplot_err_p_list, nrow = length(ggplot_err_p_list))) #generates g
    ggsave(
      sprintf(
        "%s/%s_rolling_err_metrics.png",
        png_file_dir,
        out_file_desc_prefix
      ),
      g,
      width = 12,
      height = 6
    )
    
    # select best model by CV mean of error metrics
    cur_CV_mean_mape <-
      sapply(model_mape_list, function(x) {
        mean(unlist(x), na.rm = TRUE)
      })
    cur_CV_best_mape_idx <- which.min(cur_CV_mean_mape)
    info(
      logger,
      sprintf(
        "%s '%s' best prediction model is '%s' according to MAPE metric %.4f",
        cur_bid_name,
        cur_pid_name,
        names(cur_CV_best_mape_idx),
        cur_CV_mean_mape[cur_CV_best_mape_idx]
      )
    )
    
    cur_CV_last_k_fold_mean_mape <- NA
    cur_CV_last_k_fold_best_mape_idx <- NA
    cur_CV_last_k_fold_mean_rmse <- NA
    cur_CV_last_k_fold_best_rmse_idx <- NA
    cur_best_last_k_fold_RMSE <- NA
    cur_best_last_k_fold_MAPE <- NA
    if (cur_last_k_fold_stat_valid) {
      cur_CV_last_k_fold_mean_mape <-
        sapply(model_mape_list, function(x) {
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
      
      cur_CV_last_k_fold_mean_rmse <-
        sapply(model_rmse_list, function(x) {
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
    } # cur_last_k_fold_stat_valid
    
    cur_CV_mean_rmse <-
      sapply(model_rmse_list, function(x) {
        mean(unlist(x), na.rm = TRUE)
      })
    cur_CV_best_rmse_idx <- which.min(cur_CV_mean_rmse)
    info(
      logger,
      sprintf(
        "%s '%s' best prediction model is '%s' according to RMSE metric %.4f",
        cur_bid_name,
        cur_pid_name,
        names(cur_CV_best_rmse_idx),
        cur_CV_mean_rmse[cur_CV_best_rmse_idx]
      )
    )
    
    
    cur_mean_abs_vibr_ratio_list <-
      mean_abs_vibr_ratio_list[[cur_bid_str]][[cur_pid_str]]
    cur_mean_abs_vibr_ratio <-
      mean(unlist(cur_mean_abs_vibr_ratio_list), na.rm = TRUE)
    
    cur_cv_info <-
      list(
        pid = cur_pid_str,
        pid.name = cur_pid_name,
        data.set = cur_hourly_vv_df,
        CV.seq = cur_train_h_seq,
        best_RMSE = list(
          name = names(cur_CV_best_rmse_idx),
          value = cur_CV_mean_rmse[cur_CV_best_rmse_idx]
        ),
        best_MAPE = list(
          name = names(cur_CV_best_mape_idx),
          value = cur_CV_mean_mape[cur_CV_best_mape_idx]
        ),
        best_last_k_fold_RMSE = cur_best_last_k_fold_RMSE,
        best_last_k_fold_MAPE = cur_best_last_k_fold_MAPE,
        last.k.RMSE = cur_CV_last_k_fold_mean_rmse,
        last.k.MAPE = cur_CV_last_k_fold_mean_mape,
        daily.pred.plot.mat = daily_pred_plot_mat,
        pred.plot.mat = hourly_pred_plot_mat,
        rmse.vec.list = lapply(model_rmse_list, unlist),
        mape.vec.list = lapply(model_mape_list, unlist),
        start.date = cur_start_date_str,
        end.date = cur_pred_end_date_str,
        updateTime_lab = cur_pid_updateTime,
        mean_abs_vibr_ratio = cur_mean_abs_vibr_ratio
      )
    best_model_list[[cur_bid_str]][[cur_pid_str]] = cur_cv_info
    
    # cat("\n")
    info(logger, "\n")
  } # for every pid
  mysql_dbDisconnectAll(mysql.drv)
} # for every bid


# start CV ggplot
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
    cur_CV_cnt <-
      length(cur_cv_info$CV.seq)
    model_heatmap_df <-
      cbind(model_heatmap_df,
            sapply(cur_cv_info$mape.vec.list, mean, na.rm = TRUE))
    colnames(model_heatmap_df)[ncol(model_heatmap_df)] <-
      cur_pid_name
    
    cur_model_comp_m <-
      melt(cur_mape_vec_list)
    colnames(cur_model_comp_m)[ncol(cur_model_comp_m)] <-
      "model.name"
    ggplot(data = subset(cur_model_comp_m,!is.na(value)),
           aes(x = model.name, y = value, fill = model.name)) +
      geom_boxplot() +  guides(fill = guide_legend(title = "模型类别")) +
      labs(
        title = sprintf(
          "%s '%s' %s-%s CV MAPE Metric",
          cur_bid_name,
          cur_pid_name,
          cur_start_date_str,
          cur_end_date_str
        )
      ) +
      ylab("模型名称") + xlab("24小时预测误差MAPE") +
      theme(axis.text.x = element_text(angle = 60,
                                       # vjust = 0.5,
                                       hjust = 1))
    # ggplotly() # interactive boxplot for RStudio
    ggsave(
      sprintf(
        "%s/%s_%s_%s-%s_%d-fold_CV_model_comp_boxplot.png",
        png_file_dir,
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_end_date_str,
        cur_CV_cnt
      ),
      width = 12,
      height = 6
    )
    
    # plot model mean running time for every terminal
    cur_model_fit_time_m <-
      melt(model_fit_time_list[[cur_bid_str]][[cur_pid_str]])  # this conversion will IGNORE different time units!
    cur_model_fit_time_m$L3 <- NULL
    cur_model_fit_time_m$pid_name <-
      rep(cur_pid_name, nrow(cur_model_fit_time_m))
    names(cur_model_fit_time_m) <-
      c("difftime", "model.name", "train_size", "pid.name")
    cur_model_fit_time_m_sp <-
      split.data.frame(cur_model_fit_time_m, cur_model_fit_time_m$model.name)
    cur_model_fit_time_plot_df <-
      do.call(rbind, lapply(cur_model_fit_time_m_sp, function(df) {
        df$train_size <- NULL
        # https://stackoverflow.com/questions/12611361/r-find-time-difference-in-seconds-for-yyyy-mm-dd-hhmmss-mmm-format#comment-50651987
        df$difftime <- as.numeric(df$difftime, units = "secs")
        df
      }))
    ggplot(
      data = subset(cur_model_fit_time_plot_df,!is.na(difftime)),
      aes(x = model.name, y = difftime, fill = model.name)
    ) + geom_boxplot() +
      theme(axis.text.x = element_text(angle = 60,
                                       # vjust = 0.5,
                                       hjust = 1)) + xlab("模型名称") + ylab("运行时间(秒)") +
      labs(title = sprintf(
        "%s端 '%s' %d-fold 模型拟合时间比较",
        cur_bid_name,
        cur_pid_name,
        cur_CV_cnt
      ))
    ggsave(
      sprintf(
        "%s/%s_%s_%s-%s_%d-fold_CV_model_fit_time_boxplot.png",
        png_file_dir,
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_end_date_str,
        cur_CV_cnt
      ),
      width = 12,
      height = 6
    )
  } # for every pid
  
  cur_time <- Sys.time()
  cur_time_str <- format(cur_time, "%Y%m%d-%H:%M")
  
  model_fit_time_m <- melt(model_fit_time_list[[cur_bid_str]])
  model_fit_time_m$L4 <- NULL
  names(model_fit_time_m) <-
    c("difftime", "model.name", "train_size", "pid")
  model_fit_time_m$pid.name <-
    apply(model_fit_time_m, 1, function(row) {
      unlist(subset(clip_info_list[[cur_bid_str]], clipId == as.integer(row[["pid"]]), sel = clipName))
    })
  model_fit_time_m$difftime <-
    as.numeric(model_fit_time_m$difftime, units = "secs")
  model_fit_time_mean_df <-
    aggregate(
      model_fit_time_m$difftime,
      by = list(
        model.name = model_fit_time_m$model.name,
        pid.name = model_fit_time_m$pid.name
      ),
      FUN = mean,
      na.rm = TRUE
    )
  colnames(model_fit_time_mean_df)[grep("x", names(model_fit_time_mean_df))] <-
    "mean_secs"
  ggplot(
    data = subset(model_fit_time_mean_df, !is.na(mean_secs)),
    aes(x = model.name, y = mean_secs, fill = model.name)
  ) +
    geom_boxplot() +  guides(fill = guide_legend(title = "模型类别")) +
    theme(axis.text.x = element_text(angle = 60,
                                     # vjust = 0.5,
                                     hjust = 1)) + xlab("模型名称") + ylab("运行时间(秒)") +
    labs(
      title = sprintf(
        "%s top%d clipId/pId model %d-fold CV fit time",
        cur_bid_name,
        top_pid_cnt,
        cur_CV_cnt
      )
    ) +
    ylab("模型拟合时间(秒)") + xlab("模型名称")
  # ggplotly() # interactive boxplot for RStudio
  ggsave(
    sprintf(
      "%s/%s_top%d_%d-fold_model_fit_time_boxplot_%s.png",
      png_file_dir,
      cur_bid_name,
      top_pid_cnt,
      cur_CV_cnt,
      cur_time_str
    ),
    width = 12,
    height = 6
  )
  
  model_heatmap_m <- melt(model_heatmap_df)
  colnames(model_heatmap_m)[1:2] <- c("model.name", "pid.name")
  model_heatmap_m2 <-
    model_heatmap_m %>% group_by(model.name) %>% dplyr::mutate(rescale = rescale(value))
  base_size <- 9
  p <- ggplot(model_heatmap_m2, aes(model.name, pid.name)) +
    geom_tile(aes(fill = rescale), colour = "yellow") +
    scale_fill_gradient(low = "red", high = "steelblue") +
    guides(fill = guide_legend(title = "标准化后的MAPE误差值")) +
    theme(axis.text.x = element_text(angle = 60,
                                     # vjust = 0.5,
                                     hjust = 1)) +
    ylab("合集名称") + xlab("模型名称") +
    labs(title = sprintf("%s端top %d合集模型CV MAPE指标比较", cur_bid_name, top_pid_cnt))
  p <- p + theme(
    # legend.position = "none",
    axis.ticks = element_blank(),
    axis.text.x = element_text(
      size = base_size * 0.8,
      angle = 330,
      hjust = 0,
      colour = "grey50"
    )
  )
  ggsave(
    sprintf(
      "%s/%s_top%d_%d-fold_model_comp_heatmap_%s.png",
      png_file_dir,
      cur_bid_name,
      top_pid_cnt,
      cur_CV_cnt,
      cur_time_str
    ),
    width = 12,
    height = 6
  )
  
  ggplot(data = model_heatmap_m, aes(x = model.name, y = value, fill = model.name)) +
    geom_boxplot() +  guides(fill = guide_legend(title = "模型类别")) +
    theme(axis.text.x = element_text(angle = 60,
                                     # vjust = 0.5,
                                     hjust = 1)) +
    labs(
      title = sprintf(
        "%s top%d clipId/pId model %d-fold CV Error Metric",
        cur_bid_name,
        top_pid_cnt,
        cur_CV_cnt
      )
    ) +
    ylab("模型名称") + xlab("24小时预测误差MAPE") +
    # ggplotly() # interactive boxplot for RStudio
    ggsave(
      sprintf(
        "%s/%s_top%d_%d-fold_model_comp_boxplot_%s.png",
        png_file_dir,
        cur_bid_name,
        top_pid_cnt,
        cur_CV_cnt,
        cur_time_str
      ),
      width = 12,
      height = 6
    )
} # for every bid

# write aggregated stat result
cur_time_str <- format(cur_time, "%Y%m%d-%H:%M")
for (cur_bid_idx in seq_along(names(best_model_list))) {
  cur_bid_str = bid_str_list[[cur_bid_idx]]
  cur_bid_name <- bid_name_list[[cur_bid_idx]]
  cur_time <- Sys.time()
  cur_rmse_stat_file <-
    sprintf(
      "%s/%s_top%d_CV_RMSE_stat_%s",
      stat_file_dir,
      cur_bid_name,
      top_pid_cnt,
      cur_time_str
    )
  rmse_file_con <- file(cur_rmse_stat_file, 'w')
  write(paste(
    c(
      "pid",
      "pid_name",
      "start_date",
      "pred_end_date",
      "best_model_name",
      "value",
      sprintf("best_last_k-fold_model_name", last_k_fold_stat_num),
      sprintf("last_k-fold_value", last_k_fold_stat_num)
    )
    ,
    collapse = "\t"
  ),
  cur_rmse_stat_file,
  append = TRUE,
  sep = "\n")
  cur_mape_stat_file <-
    sprintf(
      "%s/%s_top%d_CV_MAPE_stat_%s",
      stat_file_dir,
      cur_bid_name,
      top_pid_cnt,
      cur_time_str
    )
  mape_file_con <- file(cur_mape_stat_file, 'w')
  write(paste(
    c(
      "pid",
      "pid_name",
      "start_date",
      "pred_end_date",
      "best_model_name",
      "value",
      "mean_vibr_ratio",
      sprintf("best_last_%d-fold_model_name", last_k_fold_stat_num),
      sprintf("last_%d-fold_value", last_k_fold_stat_num)
    ),
    collapse = "\t"
  ),
  cur_mape_stat_file,
  append = TRUE,
  sep = "\n")
  for (cur_pid_idx in seq_along(names(best_model_list[[cur_bid_str]]))) {
    cur_pid_str <- names(best_model_list[[cur_bid_str]])[[cur_pid_idx]]
    cur_cv_info <- best_model_list[[cur_bid_str]][[cur_pid_str]]
    cur_test_mean_abs_vibr_ratio <- cur_cv_info$mean_abs_vibr_ratio
    cur_CV_cnt <- length(cur_cv_info$CV.seq)
    stopifnot(!is.null(cur_cv_info))
    cur_best_rmse_info <-
      cur_cv_info$best_RMSE
    cur_last_k_fold_best_rmse_info <-
      cur_cv_info$best_last_k_fold_RMSE
    if (is.na(cur_last_k_fold_best_rmse_info)){
      write(
        sprintf(
          # "%s\t%s\t%s\t%s\t%s\t%.4f\tNA\tNA",
          cur_cv_info$pid,
          cur_cv_info$pid.name,
          cur_cv_info$start.date,
          cur_cv_info$end.date,
          cur_best_rmse_info$name,
          cur_best_rmse_info$value
          ),
        cur_rmse_stat_file,
        append = TRUE,
        sep = "n"
      )
    } else{
      write(
        sprintf(
          "%s\t%s\t%s\t%s\t%s\t%.4f\t%s\t%.4f",
          cur_cv_info$pid,
          cur_cv_info$pid.name,
          cur_cv_info$start.date,
          cur_cv_info$end.date,
          cur_best_rmse_info$name,
          cur_best_rmse_info$value,
          cur_last_k_fold_best_rmse_info$name,
          cur_last_k_fold_best_rmse_info$value
        ),
        cur_rmse_stat_file,
        append = TRUE,
        sep = "n"
      )
    }
    
    cur_best_mape_info <-
      cur_cv_info$best_MAPE
    cur_last_k_fold_best_mape_info <-
      cur_cv_info$best_last_k_fold_MAPE
    if (is.na(cur_last_k_fold_best_mape_info)){
      write(
        sprintf(
          "%s\t%s\t%s\t%s\t%s\t%.4f\t%.4f\tNA\tNA",
          cur_cv_info$pid,
          cur_cv_info$pid.name,
          cur_cv_info$start.date,
          cur_cv_info$end.date,
          cur_best_mape_info$name,
          cur_best_mape_info$value,
          cur_test_mean_abs_vibr_ratio
          ),
        cur_mape_stat_file,
        append = TRUE,
        sep = "n"
      )
    } else{
      write(
        sprintf(
          "%s\t%s\t%s\t%s\t%s\t%.4f\t%.4f\t%s\t%.4f",
          cur_cv_info$pid,
          cur_cv_info$pid.name,
          cur_cv_info$start.date,
          cur_cv_info$end.date,
          cur_best_mape_info$name,
          cur_best_mape_info$value,
          cur_test_mean_abs_vibr_ratio,
          cur_last_k_fold_best_mape_info$name,
          cur_last_k_fold_best_mape_info$value
        ),
        cur_mape_stat_file,
        append = TRUE,
        sep = "n"
      )
    }
    
  }
  for (pid_idx in seq_along(skipped_pid_list[[cur_bid_str]])) {
    pid <- skipped_pid_list[[cur_bid_str]][[pid_idx]]
    pid_name <- skipped_pid_name_list[[cur_bid_str]][[pid_idx]]
    write(
      sprintf(
        "%s\t'%s'\tNA\tNA\tNA\tNA\tNA\tNA\tNA",
        pid,
        pid_name,
        skipped_pid_train_size_list[[cur_bid_str]][[pid_idx]]
      ),
      cur_mape_stat_file,
      append = TRUE,
      sep =
        "\n"
    )
    write(
      sprintf(
        "%s\t'%s'\tNA\tNA\tNA\tNA\tNA\tNA",
        pid,
        pid_name,
        skipped_pid_train_size_list[[cur_bid_str]][[pid_idx]]
      ),
      cur_rmse_stat_file,
      append = TRUE,
      sep =
        "\n"
    )
  }
  if (length(skipped_pid_name_list)) {
    cat(sprintf(
      "Skipped TV program: '%s'\n",
      paste(skipped_pid_name_list[[cur_bid_str]], ",")
    ))
  }
  close(rmse_file_con)
  close(mape_file_con)
}

dput(
  best_model_list,
  sprintf(
    "%s/top%d_model_cv_info_%s",
    dput_file_dir,
    top_pid_cnt,
    cur_time_str
  )
)
dput(
  xreg_info_list,
  sprintf(
    "%s/top%d_xreg_info_%s",
    dput_file_dir,
    top_pid_cnt,
    cur_time_str
  )
)
dput(
  model_fit_time_list,
  sprintf(
    "%s/top%d_model_fit_time_%s",
    dput_file_dir,
    top_pid_cnt,
    cur_time_str
  )
)

tryCatch({
  mysql_dbDisconnectAll(mysql.drv)  # timeout or have pending rows
}, error = function (e) {
  error(logger, as.character(e))
})
tryCatch({
  postgresql_dbDisconnectAll(hourly.vv.pg.drv)  # timeout or have pending rows
}, error = function (e) {
  error(logger, as.character(e))
})
tryCatch({
  postgresql_dbDisconnectAll(daily.vv.pg.drv)
}, error = function (e) {
  error(logger, as.character(e))
})

program.end.time <- Sys.time()
program.time.taken <- program.end.time - program.start.time
info(logger, sprintf("Total program running time: %s", format(program.time.taken)))