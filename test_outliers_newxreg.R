#!/usr/bin/Rscript
# sudo yum install -y postgresql-devel libcurl-devel
require("RPostgreSQL")
library('labeling', lib = "/usr/lib64/R/library")
library('digest', lib = "/usr/lib64/R/library")
library('ggplot2', lib = "/usr/lib64/R/library")
library('forecast', lib = "/usr/lib64/R/library")
library('tseries', lib = "/usr/lib64/R/library")
library('Metrics')
library('zoo')
library('xts')
library("QuantPsyc") # lm.beta for standardized coefficients
require("MASS")
require('sys')
require("RMySQL")
require("log4r")

rm(list = ls())

# script.dir <- dirname(sys.frame(1)$ofile) # not working from linux cli
script.dir <- getwd()
cat(script.dir)
wd <- script.dir
setwd(wd)

options(error = quote({
  dump.frames()
  save.image(file = "last.dump.rda")
})) # https://stackoverflow.com/questions/40421552/r-how-make-dump-frames-include-all-variables-for-later-post-mortem-debugging#40431711
# options(error = traceback) # for interactive environment

# options(download.file.method = "wget")
# Chinese fonts installation: sudo yum install -y "cjkuni*" "wqy-zenhei-fonts"
#require("textclean")

## defined in library "forecast"
# invBoxCox <- function(x, lambda){
#   if (lambda == 0) exp(x) else (lambda*x + 1)^(1/lambda)
# }


mysql_dbDisconnectAll <- function(drv) {
  ile <- length(dbListConnections(drv))
  lapply(dbListConnections(drv), function(x)
    dbDisconnect(x))
  cat(sprintf("%s connection(s) closed.\n", ile))
}

postgresql_dbDisconnectAll <- function(drv) {
  for (con in dbListConnections(drv)) {
    dbDisconnect(con)
  }
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
  df
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


vv_stat_days_count <- 30
top_pid_cnt <- 30
max_train_size <- 14
bid_list_str_list <- c("(9)", "(12)")  # aPhone, iPhone
bid_name_list <- c("aPhone", "iPhone")
stat_file_list <- c()
stat_file_dir_prefix <- "out/stat"
dir.create(stat_file_dir_prefix, showWarnings = FALSE)
xreg_stat_file_dir_prefix <- "out/stat/xreg"
dir.create(xreg_stat_file_dir_prefix, showWarnings = FALSE)
error_file_dir_prefix <- "out/error"
dir.create(error_file_dir_prefix, showWarnings = FALSE)
png_file_dir_prefix <- "out/png"
dir.create(png_file_dir_prefix, showWarnings = FALSE)
for (bid_name in bid_name_list) {
  cur_time <- Sys.time()
  cur_time_str <- format(cur_time, "%Y%m%d-%H:%M")
  cur_stat_file <-
    sprintf("%s/%s_top%d_%s_stat",
            stat_file_dir_prefix,
            bid_name,
            top_pid_cnt,
            cur_time_str)
  stat_file_list <- c(stat_file_list, cur_stat_file)
}
# max_p <- 5 # default value
# max_q <- 5 # default value
# max_P <- 2  # default value
# max_Q <- 2 # default value
# max_order <- 5 # default value
# max_d <- 2 # default value
# max_D <- 1 # default value

max_p <- 7 # experimental, significantly slower than default value, especially for seasonal model
max_q <- 7 # experimental
max_P <- 7
max_Q <- 7
max_d <- 2
max_D <- 2
max_order <- max_p + max_q + max_P + max_Q

using_xreg <- TRUE
min_observations <- 14

logger <- create.logger(logfile = "", level = "INFO")

# create a connection
# save the password that we can "hide" it as best as we can by collapsing it
pw <- {
  "Dqi2Si3EER_ja983KBDFg"
}
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(
  drv,
  dbname = "dm_result",
  host = "10.100.5.85",
  port = 2345,
  user = "analyse_readonly",
  password = pw
)

rm(pw) # removes the password


yesterday <- Sys.Date() - 1
yesterday_str <- format(yesterday, "%Y%m%d")
vv_start_date <- yesterday - vv_stat_days_count + 1
vv_start_date_str <- format(vv_start_date, "%Y%m%d")
vv_end_date_str <- yesterday_str
# pid_str_list <-
#   c(
#     "308710",
#     "308734",
#     "317663",
#     "316045",
#     "316685",
#     "316387",
#     "318076",
#     "318221",
#     "167448",
#     "316670",
#     "319268",
#     "318795",
#     "299619"
#   )

# require("caret")
# https://www.timeanddate.com/holidays/china/
holidays_df <-
  read.table(
    "~/R_BoxCox_arima_batch_prediction/chinese_holidays",
    sep = "\t",
    quote = "",
    header = TRUE
  )
holidays_df$Date <- as.Date(strptime(holidays_df$Date, "%Y %b %d"))
# holidays_df$Weekday_index <- format(holidays_df$Date, "%w")
# weekday_index_df <- as.data.frame(holidays_df$Weekday_index)
# encoder <- dummyVars("~.", data = weekday_index_df)
# encoded_weekday_index_df <- predict(encoder, newdata = weekday_index_df)


yesterday <- Sys.Date() - 1
yesterday_str <- format(Sys.Date() - 1, "%Y%m%d")
last_day_of_year <- strptime("2017-12-31", "%Y-%m-%d")
# ## ifelse() strips attributes
# ## This is important when working with Dates and factors
# if (yesterday < last_day_of_year){
#   last_feasible_day <- yesterday
# } else {
#   last_feasible_day <- last_day_of_year
# }
last_feasible_day <- yesterday

mysql.drv <- MySQL()
mydb = dbConnect(
  mysql.drv,
  user = 'bigdata_r',
  password = 'B1gD20t0_r',
  port = 3306,
  host = '10.27.106.230',
  dbname = "mgmd"
)


model_info_list <- c(c())
bid_list_str_num <- length(bid_list_str_list)
total_task_num <- bid_list_str_num * top_pid_cnt
skipped_pid_list <- c(c())
skipped_pid_name_list <- c(c())
skipped_pid_train_size_list <- c(c())
for (j in seq_along(bid_list_str_list)) {
  cur_bid_str <- bid_list_str_list[j]
  cur_bid_name <- bid_name_list[j]
  
  vv_sql <-
    sprintf(
      "select pid, sum(vv) as vv from vv_pid_day where bid in (%s) and date between %s and %s and pid != -1 group by pid order by vv desc limit %d; ",
      cur_bid_str,
      vv_start_date_str,
      vv_end_date_str,
      top_pid_cnt
    )
  print(vv_sql)
  
  # query the top pid from postgreSQL
  sql.start.time <- Sys.time()
  pid_vv_data <- dbGetQuery(con, vv_sql)
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
  
  # pid_str_list <- c("308703", "316387", "317663", "319812", "320519", "320146")
  
  for (i in seq_along(pid_str_list)) {
    # for every pid
    mape_list <- c()
    first_day_ape_list <- c()
    xreg_fitted_mape_list <- c()
    model_list <- c()
    forecast_list <- c()
    model_name_list <- c()
    plot_forecast_error_data_list <- c()
    forecast_title_list <- c()
    forecast_actual_data_list <- c()
    
    sql <-
      sprintf(
        'select clipId, clipName, updateTime, releaseTime  from asset_v_clips where clipId in %s;',
        paste0('(', paste(pid_str_list, collapse = ","), ')')
      )
    print(sql)
    dbSendQuery(mydb, "SET NAMES utf8")
    res_df <- dbGetQuery(mydb, sql)
    res_df$updateTime <-
      strptime(res_df$updateTime, format = "%Y-%m-%d %H:%M:%S")
    res_df$releaseTime <-
      strptime(res_df$releaseTime, format = "%Y-%m-%d %H:%M:%S")
    
    start_date_str_list <-
      format(res_df$releaseTime, format = "%Y%m%d")
    predict_ts_len <- 7
    start_date <- as.Date(res_df$releaseTime)
    end_date <- as.Date(res_df$updateTime)
    # predict_end_date <- end_date + predict_ts_len
    if (is.null(max_train_size)) {
      predict_end_date <- end_date + predict_ts_len
    } else {
      predict_end_date <-
        start_date + (max_train_size - 1) + predict_ts_len
    }
    predict_end_date <-
      as.Date(unlist(lapply(predict_end_date, function(x)
        ifelse(x > last_feasible_day, last_feasible_day, x))))
    pred_end_date_str_list <-
      format(predict_end_date, format = "%Y%m%d")
    pid_name_list <- res_df$clipName
    pid_name_list <-
      gsub(" ", "", pid_name_list, fixed = TRUE)  # remove white spaces
    pid_str_list <- as.character(res_df$clipId)
    
    cur_start_date_str <- start_date_str_list[i]
    cur_pred_end_date_str <- pred_end_date_str_list[i]
    days_count = predict_end_date[i] - start_date[i] + 1
    cur_pid <- pid_str_list[i]
    cur_pid_name <- pid_name_list[i]
    cur_pid_num <- length(pid_str_list)
    
    png_predict_file_prefix <-
      sprintf(
        "%s_%s_%s-%s_%sdays-holdout",
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        predict_ts_len
      )
    out_file_desc_prefix <-
      sprintf(
        "%s_%s_%s-%s_%d-points",
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        days_count
      )
    png_file_path_prefix <-
      sprintf("%s/%s", png_file_dir_prefix, out_file_desc_prefix)
    png_predict_file_path_prefix <-
      sprintf("%s/%s", png_file_dir_prefix, png_predict_file_prefix)
    cur_err_out <-
      sprintf("%s/%s_err", error_file_dir_prefix, out_file_desc_prefix)
    xreg_stat_file_path <- sprintf("%s/%s_xreg_rela_impo", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    xreg_lm_png_file_path_prefix <- sprintf("%s/%s_xreg_lm_plot", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    fourier_xreg_lm_png_file_path_prefix <- sprintf("%s/%s_fourier_xreg_lm_plot", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    bc_xreg_stat_file_path <- sprintf("%s/%s_boxcox_xreg_rela_impo", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    bc_xreg_lm_png_file_path_prefix <- sprintf("%s/%s_boxcox_xreg_lm_plot", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    bc_fourier_xreg_lm_png_file_path_prefix <- sprintf("%s/%s_boxcox_fourier_xreg_lm_plot", xreg_stat_file_dir_prefix, out_file_desc_prefix)
    stopifnot(cur_start_date_str < cur_pred_end_date_str)
    
    select_sql <- sprintf(
      "select date,sum(vv) as vv from vv_pid_day where bid in %s and date between %s and %s and pid = %s group by date, pid order by date, vv desc;",
      cur_bid_str,
      cur_start_date_str,
      cur_pred_end_date_str,
      cur_pid
    )
    cur_task_cnt <- (j - 1) * top_pid_cnt + i
    info(
      logger,
      sprintf(
        "%d(%.2f%%).\t %s '%s' %s",
        i,
        (cur_task_cnt * 1.0) / total_task_num * 100,
        cur_bid_name,
        cur_pid_name,
        select_sql
      )
    )
    # query daily VV data from postgreSQL
    sql.start.time <- Sys.time()
    cur_daily_data <- dbGetQuery(con, select_sql)
    sql.end.time <- Sys.time()
    sql.time.taken <- sql.end.time - sql.start.time
    
    cur_observation_num <- length(cur_daily_data$vv)
    cur_sample_size <- cur_observation_num
    info(
      logger,
      sprintf(
        "%s '%s'查询%s-%s每日VV 获得%d条数据, 耗时:\n%s",
        cur_bid_name,
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        cur_observation_num,
        format(sql.time.taken)
      )
    )
    
    if (cur_observation_num < vv_stat_days_count) {
      info(
        logger,
        sprintf(
          "%s '%s' %s-%s has updated within %d days, %d observations",
          cur_bid_name,
          cur_pid_name,
          cur_start_date_str,
          cur_pred_end_date_str,
          predict_ts_len,
          cur_observation_num
        )
      )
    }
    if (nrow(cur_daily_data) == 0) {
      info(
        logger,
        sprintf(
          "%s '%s' %s-%s NO VV data",
          cur_bid_name,
          cur_pid_name,
          cur_start_date_str,
          cur_pred_end_date_str
        )
      )
      next
    }
    cur_daily_data$date = as.character(cur_daily_data$date)
    cur_daily_data$Date = as.Date(cur_daily_data$date, format = "%Y%m%d")
    predict_date <- tail(cur_daily_data$Date, predict_ts_len)
    
    ggplot(cur_daily_data, aes(Date, vv)) + geom_line() + scale_x_date('day')  + ylab("aphone daily VV")
    
    cur_daily_vv <- cur_daily_data[, c('vv')]
    count_xts = xts(cur_daily_vv, order.by = cur_daily_data$Date)
    start_year <-
      as.numeric(format(cur_daily_data$Date[1], "%Y"))
    start_date_i <-
      as.numeric(format(cur_daily_data$Date[1], "%j"))
    last_date_index <- length(cur_daily_data$Date)
    end_year <-
      as.numeric(format(cur_daily_data$Date[last_date_index], "%Y"))
    end_date_i <-
      as.numeric(format(cur_daily_data$Date[last_date_index], "%j"))
    
    # # 9: stop("series is not periodic or has less than two periods")
    # # 8: stl(ts(x.bc, frequency = freq), "per")
    # # 7: bld.mbb.bootstrap(y, 100)
    # # 6: lapply(bootstrapped_series, function(x) {
    # #   mod <- ets(x, ...)
    # # })
    # # 5: baggedETS(cur_train_sample) at test_outliers_newxreg.R#1851
    # NO support for daily ts plot with date label currently
    # count_ts = ts(
    #   data = cur_daily_vv,
    #   start = c(start_year, start_date_i),
    #   end = c(end_year, end_date_i),
    #   frequency = 365
    #   # frequency = 365.25 #Error in attr(data, "tsp") <- c(start, end, frequency) :
    #   #   # invalid time series parameters specified
    # ) # might diff than original data for unknown reason
    
    vv_count_ts <- ts(data = cur_daily_vv) # frequency = 1
    # clean_cnt_ts = tsclean(count_ts) # replace by "tsoutliers"
    weekly_ts <- ts(vv_count_ts, frequency = 7)
    cur_daily_data$clean_cnt  <-  vv_count_ts
    cur_daily_data$clean_cnt_change_ratio <-
      as.numeric(vv_count_ts) / lag(as.numeric(vv_count_ts), 1) - 1
    cur_daily_data$cnt_ma = ma(vv_count_ts, order = 7) # using the clean count with no outliers
    if (length(vv_count_ts) > 30)
      cur_daily_data$cnt_ma30 = ma(vv_count_ts, order = 30)
    count_ma_ts = ts(na.omit(cur_daily_data$cnt_ma) , frequency = 7) # get rid of "Error in na.fail.default(as.ts(x)) : missing values in object"
    p <- ggplot() +
      geom_line(data = cur_daily_data, aes(x = Date, y = clean_cnt)) + scale_x_date("日期") + ylab('aphone daily VV')
    # ggsave(
    #   paste0(png_predict_file_prefix, "_input_tseries_data.png"),
    #   width = 12,
    #   height = 6
    # )
    
    p <- ggplot() +
      geom_line(data = cur_daily_data, aes(x = Date, y = clean_cnt, colour = "clean VV count"))
    if (length(cur_daily_data$clean_cnt) >= 7) {
      p <- p + geom_line(data = cur_daily_data,
                         aes(
                           x = Date,
                           y = rollmean(clean_cnt, 7, na.pad = TRUE),
                           colour = "Weekly Moving Average"
                         ))
    }
    if (length(cur_daily_data$clean_cnt) >= 30) {
      p <- p + geom_line(data = cur_daily_data,
                         aes(
                           x = Date,
                           y = rollmean(clean_cnt, 30, na.pad = TRUE),
                           colour = "Monthly Moving Average"
                         ))
    }
    p <- p + ylab('time series Count') +
      labs(caption = out_file_desc_prefix)
    ggsave(
      paste0(png_file_path_prefix, "_input_tseries_ma.png"),
      width = 12,
      height = 6
    )
    
    clean_cnt_len <- length(vv_count_ts)
    train_size <- clean_cnt_len - predict_ts_len
    if (train_size < 0 || train_size < min_observations) {
      warn(
        logger,
        sprintf(
          "'%s' has only %d observations, skipped.",
          cur_pid_name,
          cur_observation_num
        )
      )
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(cur_pid)) # more elements supplied than there are to replace otherwise
      skipped_pid_name_list[[cur_bid_str]] <-
        c(skipped_pid_name_list[[cur_bid_str]], list(cur_pid_name))
      skipped_pid_train_size_list[[cur_bid_str]] <-
        c(skipped_pid_train_size_list[[cur_bid_str]], list(train_size))
      # https://robjhyndman.com/papers/shortseasonal.pdf
      next
    }
    train_vv_count_ts <-
      head(vv_count_ts,-predict_ts_len)
    actual_vv_count_ts <- vv_count_ts
    train_sample_ts <- train_vv_count_ts
    test_vv_count_ts <-
      tail(actual_vv_count_ts, predict_ts_len)
    train_weekly_cnt_ts <- head(weekly_ts, -predict_ts_len)
    test_weekly_cnt_ts <- tail(weekly_ts, predict_ts_len)
    
    # https://stackoverflow.com/questions/33999512/how-to-use-the-box-cox-power-transformation-in-r
    bc <-
      boxcox(train_sample_ts ~ 1) # https://stackoverflow.com/questions/13366755/what-does-the-r-formula-y1-mean#13367260
    lambda <- bc$x[which.max(bc$y)]
    bc_train_clean_cnt_sample_ts <-
      BoxCox(train_sample_ts, lambda = lambda)
    bc_actual_vv_count_ts <-
      BoxCox(actual_vv_count_ts, lambda = lambda)
    bc_weekly_ts <- BoxCox(weekly_ts, lambda = lambda)
    bc_train_weekly_cnt_ts <- head(bc_weekly_ts, -predict_ts_len)
    bc_test_weekly_cnt_ts <- tail(bc_weekly_ts, predict_ts_len)

    cur_daily_data$is_holiday <-
      cur_daily_data$Date %in% holidays_df$Date
    levels(cur_daily_data$is_holiday) <- list(TRUE, FALSE)
    cur_daily_data$weekday_str = format(cur_daily_data$Date, "%A")
    levels(cur_daily_data$weekday_str) <-
      list(
        Mon = "Monday",
        Tue = "Tuesday",
        Wed = "Wednesday",
        Thu = "Thursday",
        Fri = "Friday",
        Sat = "Saturday",
        Sun = "Sunday"
      )
    cur_daily_data$weekday_levels <- as.factor(cur_daily_data$weekday_str)
    cur_daily_data$day <-
      cur_daily_data$Date - cur_daily_data$Date[[1]] + 1
    xreg <- NULL
    fxreg <- NULL
    bc_xreg <- NULL
    bc_fxreg <- NULL
    fourier_xreg <- NULL
    fourier_fxreg <- NULL
    cur_xreg_fourier <- NULL
    cur_fxreg_fourier <- NULL
    cur_bc_xreg_fourier <- NULL
    cur_bc_fxreg_fourier <- NULL
    cur_train_sample <- train_sample_ts
    cur_test_sample <- test_vv_count_ts
    if (using_xreg) {
      # label outliers's type for xreg
      cur_tsoutliers <- NULL
      outliers_newxreg <- NULL
      tsoutliers_exist <- TRUE
      tsoutliers_na <- FALSE
      require("tsoutliers")
      tryCatch({
        cur_tsoutliers <-
          tso(train_sample_ts, types = c("AO", "LS", "TC")) # Error in rowSums(xregg) : 'x' must be an array of at least two dimensions
        if (0 != nrow(cur_tsoutliers$outliers)) {
          outliers_newxreg <-
            outliers.effects(cur_tsoutliers$outliers,
                             train_size + predict_ts_len) # Error in oeff[, indao] <- AOeffect(n, mo[indao, "ind"], mo[indao, "coefhat"]) :
          newxreg_colnames <- colnames(outliers_newxreg)
          outliers_newxreg <-
            as.matrix(outliers_newxreg[-seq_along(train_sample_ts),])
          colnames(outliers_newxreg) <- newxreg_colnames
        } else{
          tsoutliers_exist <- FALSE
        }
        
      }, error = function(e) {
        error(logger, sprintf("tsoutliers::tso:\t'%s'", as.character(e)))
        sink(cur_err_out)
        print(as.character(e))
        print(sys.calls())
        sink()
        tsoutliers_na <<- TRUE
      }, finally = {
        
      })
      # boxcox sample version outliers dectection
      bc_tsoutliers_na <- FALSE
      cur_bc_tsoutliers <- NULL
      bc_outliers_newxreg <- NULL
      bc_tsoutliers_exist <- TRUE
      require("tsoutliers")
      tryCatch({
        cur_bc_tsoutliers <-
          tso(bc_train_clean_cnt_sample_ts, types = c("AO", "LS", "TC")) # Error in rowSums(xregg) : 'x' must be an array of at least two dimensions
        if (0 != nrow(cur_bc_tsoutliers$outliers)) {
          bc_outliers_newxreg <-
            outliers.effects(cur_bc_tsoutliers$outliers,
                             train_size + predict_ts_len) # Error in oeff[, indao] <- AOeffect(n, mo[indao, "ind"], mo[indao, "coefhat"]) :
          newxreg_colnames <- colnames(bc_outliers_newxreg)
          bc_outliers_newxreg <-
            as.matrix(bc_outliers_newxreg[-seq_along(bc_train_clean_cnt_sample_ts), ])
          colnames(bc_outliers_newxreg) <- newxreg_colnames
        } else {
          bc_tsoutliers_exist <- FALSE
        }
        
      }, error = function(e) {
        error(logger, sprintf("BoxCox tsoutliers::tso:\t'%s'", as.character(e)))
        sink(cur_err_out)
        print(as.character(e))
        print(sys.calls())
        sink()
        bc_tsoutliers_na <<- TRUE
      }, finally = {
        
      })
      
      if (!tsoutliers_na && tsoutliers_exist) {
        tsoutliers_xreg <- cur_tsoutliers$fit$xreg
        cur_xreg_var_cnt <- ifelse(is.null(xreg), 0, ncol(xreg))
        if (cur_xreg_var_cnt + ncol(tsoutliers_xreg) < train_size) {
          xreg <- cbind(xreg, tsoutliers_xreg)
          # xreg <- matrix(xreg, nrow = train_size, ncol = ncol(xreg)) # rank deficient
          fxreg <- cbind(fxreg, outliers_newxreg)
          # fxreg <-
          #   matrix(fxreg, nrow = predict_ts_len, ncol = ncol(fxreg))  # rank deficient
        }
        
        cur_xreg_fourier_var_cnt <- ifelse(is.null(cur_xreg_fourier), 0, ncol(cur_xreg_fourier))
        if (cur_xreg_fourier_var_cnt + ncol(tsoutliers_xreg) < train_size) {
          cur_xreg_fourier <-
            cbind(cur_xreg_fourier, tsoutliers_xreg)
          cur_fxreg_fourier <-
            cbind(cur_fxreg_fourier, outliers_newxreg)
        }
      }
      
      if (!bc_tsoutliers_na && bc_tsoutliers_exist) {
        bc_tsoutliers_xreg <- cur_bc_tsoutliers$fit$xreg
        cur_bc_xreg_var_cnt <- ifelse(is.null(bc_xreg), 0, ncol(bc_xreg))
        if (cur_bc_xreg_var_cnt + ncol(bc_tsoutliers_xreg) < train_size) {
          bc_xreg <- cbind(bc_xreg, bc_tsoutliers_xreg)
          # xreg <- matrix(xreg, nrow = train_size, ncol = ncol(xreg)) # rank deficient
          bc_fxreg <- cbind(bc_fxreg, bc_outliers_newxreg)
          # fxreg <-
          #   matrix(fxreg, nrow = predict_ts_len, ncol = ncol(fxreg))  # rank deficient
        }
        cur_bc_xreg_fourier_var_cnt <- ifelse(is.null(cur_bc_xreg_fourier), 0, ncol(cur_bc_xreg_fourier))
        if (cur_bc_xreg_fourier_var_cnt + ncol(bc_tsoutliers_xreg) < train_size) {
        cur_bc_xreg_fourier <-
          cbind(cur_bc_xreg_fourier, bc_tsoutliers_xreg)
        cur_bc_fxreg_fourier <-
          cbind(cur_bc_fxreg_fourier, bc_outliers_newxreg)
        }
      }
      
      # find best K for Fourier terms by minimizing fitted model's AIC/AICc 
      best_fit_K <- NULL
      best_fit <- Inf
      cur_xreg_fourier_valid <- TRUE
      cur_trial_ts <- train_weekly_cnt_ts
      cur_fit_K_start.time <- Sys.time()
      for (k in 3:5){
        if (k > frequency(cur_trial_ts)/2 ||
            2*k >= train_size) {
          break
        }#  K must be not be greater than period/2
        cur_trial_fourier <- fourier(cur_trial_ts, K = k)
        cur_trial_fit <- NULL
        tryCatch({
          cur_trial_fit <-
            auto.arima(cur_trial_ts, xreg = cur_trial_fourier)
        }, error = function(e){
          warn(logger, as.character(e))
        })
        if (!is.null(cur_trial_fit)){
          if (cur_trial_fit$aicc < best_fit ){
            best_fit <- cur_trial_fit$aicc
            best_fit_K <- k
          }
        }
      }
      if (is.infinite(best_fit)){
        cur_xreg_fourier_valid <- FALSE
      }
      if (cur_xreg_fourier_valid){
        stopifnot(!is.null(best_fit_K))
        cur_weekly_xreg_fourier <- fourier(cur_trial_ts, K = best_fit_K)
        cur_weekly_fxreg_fourier <-
          fourier(cur_trial_ts, K = best_fit_K, h = predict_ts_len)
        cur_xreg_fourier <-
          cbind(cur_xreg_fourier, cur_weekly_xreg_fourier)
        cur_fxreg_fourier <-
          cbind(cur_fxreg_fourier, cur_weekly_fxreg_fourier)
        cur_fit_K_end.time <- Sys.time()
        cur_fit_K.time.taken <-
          cur_fit_K_end.time - cur_fit_K_start.time
        info(
          logger,
          sprintf(
            "%s '%s' fit ts Fourier K parameter time taken: %s",
            cur_bid_name,
            cur_pid_name,
            format(cur_fit_K.time.taken)
          )
        )
      }
      
      
      if (train_size > 7 + 7*2) {
        best_fit_K <- NULL
        best_fit <- Inf
        cur_trial_ts <- ts(train_weekly_cnt_ts, frequency = 7 * 2)
        for (k in 3:7) {
          if (k > frequency(cur_trial_ts) / 2 ||
              2*k >= train_size) {
            break
          }#  K must be not be greater than period/2
          cur_trial_fourier <- fourier(cur_trial_ts, K = k)
          cur_trial_fit <- NULL
          tryCatch({
            cur_trial_fit <-
              auto.arima(cur_trial_ts, xreg = cur_trial_fourier)
          }, error = function(e) {
            warn(logger, as.character(e))
          })
          if (!is.null(cur_trial_fit)) {
            if (cur_trial_fit$aicc < best_fit) {
              best_fit <- cur_trial_fit$aicc
              best_fit_K <- k
            }
          }
        }
        if (is.infinite(best_fit)) {
          cur_xreg_fourier_valid <- FALSE
        }
        if (cur_xreg_fourier_valid) {
          stopifnot(!is.null(best_fit_K))
          cur_biweekly_xreg_fourier <-
            fourier(cur_trial_ts, K = best_fit_K)  # may have much correlation with weekly Fourier terms
          cur_biweekly_fxreg_fourier <-
            fourier(cur_trial_ts,
                    K = best_fit_K,
                    h = predict_ts_len)
          cur_xreg_fourier <-
            cbind(cur_xreg_fourier, cur_biweekly_xreg_fourier)
          cur_fxreg_fourier <-
            cbind(cur_fxreg_fourier, cur_biweekly_fxreg_fourier)
        }
        cur_fit_K_end.time <- Sys.time()
        cur_fit_K.time.taken <-
          cur_fit_K_end.time - cur_fit_K_start.time
        info(
          logger,
          sprintf(
            "%s '%s' fit ts Fourier K parameter time taken: %s",
            cur_bid_name,
            cur_pid_name,
            format(cur_fit_K.time.taken)
          )
        )
      }

      # boxcox version Fourier terms
      best_fit_K <- NULL
      best_fit <- Inf
      cur_bc_xreg_fourier_valid <- TRUE
      cur_trial_ts <- bc_train_weekly_cnt_ts
      cur_fit_K_start.time <- Sys.time()
      for (k in 3:5) {
        if (k > frequency(cur_trial_ts)/2 ||
            2*k >= train_size) {
          break
        }#  K must be not be greater than period/2
        cur_trial_fourier <-
          fourier(cur_trial_ts, K = k)
        cur_trial_fit <- NULL
        tryCatch({
          cur_trial_fit <-
            auto.arima(cur_trial_ts, xreg = cur_trial_fourier)
        }, error = function(e) {
          warn(logger, as.character(e))
        })
        if (!is.null(cur_trial_fit)) {
          if (cur_trial_fit$aicc < best_fit) {
            best_fit <- cur_trial_fit$aicc
            best_fit_K <- k
          }
        }
      }
      if (is.infinite(best_fit)) {
        cur_bc_xreg_fourier_valid <- FALSE
      }
      cur_fit_K_end.time <- Sys.time()
      cur_fit_K.time.taken <-
        cur_fit_K_end.time - cur_fit_K_start.time
      info(
        logger,
        sprintf(
          "%s '%s' fit BoxCox transformed ts Fourier K parameter time taken: %s",
          cur_bid_name,
          cur_pid_name,
          format(cur_fit_K.time.taken)
        )
      )
      cur_weekly_bc_xreg_fourier <- NULL
      cur_weekly_bc_fxreg_fourier <- NULL
      if (cur_bc_xreg_fourier_valid){
        stopifnot(!is.null(best_fit_K))
        cur_weekly_bc_xreg_fourier <-
          fourier(cur_trial_ts, K = best_fit_K)
        cur_weekly_bc_fxreg_fourier <-
          fourier(cur_trial_ts, K = best_fit_K, h = predict_ts_len)
        cur_bc_xreg_fourier <-
          cbind(cur_bc_xreg_fourier, cur_weekly_bc_xreg_fourier)
        cur_bc_fxreg_fourier <-
          cbind(cur_bc_fxreg_fourier, cur_weekly_bc_fxreg_fourier)
      }
      
      if (train_size > 7 + 7 * 2) {
        best_fit_K <- NULL
        best_fit <- Inf
        cur_trial_ts <- ts(bc_train_weekly_cnt_ts, frequency = 7 * 2)
        
        for (k in 3:7) {
          if (k > frequency(cur_trial_ts) / 2 ||
              2 * k >= train_size) {
            break
          }#  K must be not be greater than period/2
          cur_trial_fourier <- fourier(cur_trial_ts, K = k)
          cur_trial_fit <- NULL
          tryCatch({
            cur_trial_fit <-
              auto.arima(cur_trial_ts, xreg = cur_trial_fourier)
          }, error = function(e) {
            warn(logger, as.character(e))
          })
          if (!is.null(cur_trial_fit)) {
            if (cur_trial_fit$aicc < best_fit) {
              best_fit <- cur_trial_fit$aicc
              best_fit_K <- k
            }
          }
        }
        if (is.infinite(best_fit)) {
          cur_bc_xreg_fourier_valid <- FALSE
        }
        cur_fit_K_end.time <- Sys.time()
        cur_fit_K.time.taken <-
          cur_fit_K_end.time - cur_fit_K_start.time
        info(
          logger,
          sprintf(
            "%s '%s' fit BoxCox transformed ts Fourier K parameter time taken: %s",
            cur_bid_name,
            cur_pid_name,
            format(cur_fit_K.time.taken)
          )
        )
        cur_biweekly_bc_xreg_fourier <- NULL
        cur_biweekly_bc_fxreg_fourier <- NULL
        if (cur_bc_xreg_fourier_valid) {
          stopifnot(!is.null(best_fit_K))
          cur_biweekly_bc_xreg_fourier <-
            fourier(cur_trial_ts, K = best_fit_K)  # too much correlation with weekly Fourier terms
          cur_biweekly_bc_fxreg_fourier <-
            fourier(cur_trial_ts,
                    K = best_fit_K,
                    h = predict_ts_len)
          cur_bc_xreg_fourier <-
            cbind(cur_bc_xreg_fourier,
                  cur_biweekly_bc_xreg_fourier)
          cur_bc_fxreg_fourier <-
            cbind(cur_bc_fxreg_fourier,
                  cur_biweekly_bc_fxreg_fourier)

          #https://stats.stackexchange.com/questions/251416/how-to-perform-time-series-analysis-on-daily-data#answer-251582
          # When measuring seasonality(ie monthly) you need 3 iterations. Can you get more data? If not, then you are left to make a lot of assumptions which can be dangerous.
          #
          # By using regression, you can solve this problem. You can consider using 11 monthly dummies, 6 day of the week dummies and holiday dummies.
          # Not all may be significant. Not all may be constant(ie june is high and then becomes low).
          # You need to look for outliers and build a dummy for them. You need to look for lead and lag effects around holidays.
          # You need to consider day of the month impacts, week of the month impacts, long weekend, friday before a monday holiday, monday after a friday holiday.
          # You might have a trend or multiple trends. You might have a change in the general volume called a level shift.
        }
      }
      
      # target variable/y history value indenpendent part
      if (any(cur_daily_data$is_holiday)) {
        ## one-hoe encoding is not working for lm
        # is_holidays_str_df <- as.data.frame(cur_daily_data$is_holiday)
        # encoder <- dummyVars("~.", data = is_holidays_str_df)
        # encoded_is_holiday_df <- predict(encoder, newdata = is_holidays_str_df)
        # weekday_str_df <- as.data.frame(cur_daily_data$weekday_str)
        # encoder <- dummyVars("~.", data = weekday_str_df)
        # encoded_weekday_df <- predict(encoder, newdata = weekday_str_df)
        # cur_covariates <- cbind(encoded_weekday_df, encoded_is_holiday_df)
        #
        # http://genomicsclass.github.io/book/pages/expressing_design_formula.html
        
        # dunno why the intercept has to be removed although there might be rank deficiency, the reference level will be represented by ALL zeros while other level will use one-hot encoder
        # here's an explaination: http://www.stat.umn.edu/geyer/5102/examp/dummy.html
        
        # I would not recommend modelling weekly seasonality with dummy variables as it will take far too many degrees of freedom. A much better solution is to use Fourier terms
        # https://stackoverflow.com/questions/25328842/structure-an-xreg-parameter-with-three-dynamic-regressors-in-arima#answer-25355205
        cur_covariates <-
          cbind(
            model.matrix( ~ cur_daily_data$weekday_levels)[, -1], # remove reference level/intercept
            cur_daily_data$is_holiday,
            cur_daily_data$day
          )
        covar_ncol <- ncol(cur_covariates)
        colnames(cur_covariates)[covar_ncol - 1] <-
          c("is_public_holiday")
      } else{
        # weekday_str_df <- as.data.frame(cur_daily_data$weekday_str)
        # encoder <- dummyVars("~.", data = weekday_str_df)
        # encoded_weekday_df <- predict(encoder, newdata = weekday_str_df)
        # cur_covariates <- cbind(encoded_weekday_df)
        #
        cur_covariates <-
          cbind(model.matrix( ~ cur_daily_data$weekday_levels), cur_daily_data$day)
        cur_covariates <- cur_covariates[, -1] # remove intercept
        covar_ncol <- ncol(cur_covariates)
      } # any(cur_daily_data$is_holiday)
      colnames(cur_covariates)[1:6] <- c("is_Monday", "is_Saturday", "is_Sunday", "is_Thursday", "is_Tuesday", "is_Wednesday")
      colnames(cur_covariates)[covar_ncol] <-
        c("days_since_release")
      # remove duplicated columns if any
      cur_covariates <-
        cur_covariates[, colnames(unique(as.matrix(cur_covariates), MARGIN = 2))]
      
      ind_covariates <- head(cur_covariates,-predict_ts_len)
      f_ind_covariates <- tail(cur_covariates, predict_ts_len)
 
      # # https://stackoverflow.com/questions/4560459/all-levels-of-a-factor-in-a-model-matrix-in-r#4569239
      # attach(cur_daily_data)
      # cur_covariates_df <- data.frame(weekday_level = weekday_levels, days = day)
      # detach(cur_daily_data)
      # which.factor.columns <- sapply(cur_covariates_df, is.factor)
      # cur_covariates_factor_df <- as.data.frame(cur_covariates_df[, which.factor.columns])
      # names(cur_covariates_factor_df) <-
      #  names(cur_covariates_df)[which.factor.columns]
      # cur_contrasts_list <-
      #   lapply(cur_covariates_factor_df, contrasts, contrasts =
      #            FALSE)
      # # rank deficient fully one-hot encoded design matrix
      # cur_covariates_full_mat <- model.matrix(~ . + 0, # remove intercept
      #                                         data = cur_covariates_df,
      #                                         contrasts.arg = cur_contrasts_list)
      
      xreg <- cbind(xreg, ind_covariates)
      fxreg <- cbind(fxreg, f_ind_covariates)
      cur_xreg_valid <- TRUE
      if (train_size <= ncol(xreg)) {
        # too few data for coefficients estimation
        cur_xreg_valid <- FALSE
      }
      if (cur_xreg_valid) {
        dup_colnames <- duplicated(as.matrix(xreg), MARGIN = 2)
        stopifnot(!any(dup_colnames))
        uniq_colnames <-
          colnames(unique(as.matrix(xreg), MARGIN = 2))
        if (length(uniq_colnames)) {
          # remove duplicated columns
          xreg <- xreg[, uniq_colnames]
          fxreg <-
            fxreg[, uniq_colnames]
        }
        cur_xreg_rank <- qr(xreg)$rank
        cur_fxreg_rank <- qr(fxreg)$rank
        if (is.rank.deficient(xreg)) {
          # try to find correlated columns
          require("dplyr")
          require("reshape2")
          d_cor <- as.matrix(cor(xreg))
          d_cor_melt <- arrange(melt(d_cor),-abs(value))
          attach(d_cor_melt)
          cor_var_df <-
            d_cor_melt[which(Var1 != Var2 & value > 0.5), ]
          detach(d_cor_melt)
          if (nrow(cor_var_df)) {
            # error(logger, sprintf("xreg cor var %s", format(head(cor_var_df)))) # output format is weird
            write.table(head(cor_var_df),
                        file = logger$logfile,
                        row.names = FALSE)
          }
          # NOT a good idea to eliminate variable stepwise, see http://ellisp.github.io/blog/2017/03/12/stepwise-timeseries#more-complex-models
          require("usdm")
          v2 <- vifstep(xreg, th = 10)
          sel_colnames <- as.character(v2@results$Variables)
          excluded_colnames <- as.character(v2@excluded)
          cur_filtered_xreg <- xreg[, sel_colnames]
          if (nchar(excluded_colnames)){
            info(logger, sprintf(
              "predictors '%s' is excluded by VIF",
              paste(excluded_colnames, collapse = ",")
            ))
          }
          xreg <- cur_filtered_xreg
          fxreg <- fxreg[, sel_colnames]
          if (is.rank.deficient(xreg)) {
            cur_xreg_valid <- FALSE
          }
        }
      }
      
      
      bc_xreg <- cbind(bc_xreg, ind_covariates)
      bc_fxreg <- cbind(bc_fxreg, f_ind_covariates)
      cur_bc_xreg_valid <- TRUE
      if (train_size <= ncol(bc_xreg)) {
        # too few data for coefficients estimation
        cur_bc_xreg_valid <- FALSE
      }
      if (cur_bc_xreg_valid) {
        dup_colnames <- duplicated(as.matrix(bc_xreg), MARGIN = 2)
        stopifnot(!any(dup_colnames))
        bc_uniq_colnames <-
          colnames(unique(as.matrix(bc_xreg), MARGIN = 2))
        if (length(bc_uniq_colnames)) {
          # remove duplicated columns
          bc_xreg <- bc_xreg[, bc_uniq_colnames]
          bc_fxreg <-
            bc_fxreg[, bc_uniq_colnames]
        }
        cur_bc_xreg_rank <- qr(bc_xreg)$rank
        cur_bc_fxreg_rank <- qr(bc_fxreg)$rank
        if (is.rank.deficient(bc_xreg)) {
          # try to find correlated columns
          require("dplyr")
          require("reshape2")
          d_cor <- as.matrix(cor(bc_xreg))
          d_cor_melt <- arrange(melt(d_cor), -abs(value))
          attach(d_cor_melt)
          cor_var_df <-
            d_cor_melt[which(Var1 != Var2 & value > 0.5),]
          detach(d_cor_melt)
          if (nrow(cor_var_df)) {
            # error(logger, sprintf("BoxCox xreg cor var: %s", format(head(cor_var_df))))
            write.table(head(cor_var_df),
                        file = logger$logfile,
                        row.names = FALSE)
          }
          
          require("usdm")
          v2 <- vifstep(bc_xreg, th = 10)
          sel_colnames <- as.character(v2@results$Variables)
          excluded_colnames <- as.character(v2@excluded)
          cur_filtered_xreg <- bc_xreg[, sel_colnames]
          if (nchar(excluded_colnames)){
            info(logger, sprintf(
              "predictors '%s' is excluded by VIF",
              paste(excluded_colnames, collapse = ",")
            ))
          }
          bc_xreg <- cur_filtered_xreg
          bc_fxreg <- bc_fxreg[, sel_colnames]
          if (is.rank.deficient(bc_xreg)) {
            cur_bc_xreg_valid <- FALSE
          }
        }
      } # cur_bc_xreg_valid
      
      xreg_holiday <-
        as.matrix(head(cur_daily_data$is_holiday, -predict_ts_len))
      fxreg_holiday <-
        as.matrix(tail(cur_daily_data$is_holiday, predict_ts_len))
      colnames(xreg_holiday) <-
        colnames(fxreg_holiday) <-  c("is_holiday")
      cur_holiday_valid <- any(xreg_holiday)
      xreg_day <-
        as.matrix(head(cur_daily_data$day, -predict_ts_len))
      fxreg_day <-
        as.matrix(tail(cur_daily_data$day, predict_ts_len))
      colnames(xreg_day) <-
        colnames(fxreg_day) <-  c("days_since_release")
      
      if (cur_xreg_fourier_valid) {
        if (train_size <= ncol(cur_xreg_fourier)) {
          # too few data for coefficients estimation
          warn(logger,
               sprintf(
                 "train_size %d <= ncol(cur_xreg_fourier) %d",
                 train_size,
                 ncol(cur_xreg_fourier)
               ))
          cur_xreg_fourier_valid <- FALSE
        }
      }
      if (cur_xreg_fourier_valid) {
        if (cur_holiday_valid &&
            ncol(cur_xreg_fourier) + 1 < train_size) {
          # rank deficient
          cur_xreg_fourier <- cbind(cur_xreg_fourier, xreg_holiday)
          cur_fxreg_fourier <-
            cbind(cur_fxreg_fourier, fxreg_holiday)
        }
        if (ncol(cur_xreg_fourier) + 1 < train_size) {
          cur_xreg_fourier <- cbind(cur_xreg_fourier, xreg_day)
          cur_fxreg_fourier <- cbind(cur_fxreg_fourier, fxreg_day)
        }
        dup_colnames <-
          duplicated(as.matrix(cur_xreg_fourier), MARGIN = 2)
        stopifnot(!any(dup_colnames))
        fourier_uniq_colnames <-
          colnames(unique(as.matrix(cur_xreg_fourier), MARGIN = 2))
        if (length(fourier_uniq_colnames)) {
          # remove duplicated columns
          cur_xreg_fourier <-
            cur_xreg_fourier[, fourier_uniq_colnames]
          cur_fxreg_fourier <-
            cur_fxreg_fourier[, fourier_uniq_colnames]
        }
        cur_xreg_fourier_rank <- qr(cur_xreg_fourier)$rank
        cur_fxreg_fourier_rank <- qr(cur_fxreg_fourier)$rank
        if (cur_xreg_fourier_valid &&
            is.rank.deficient(cur_xreg_fourier)) {
          # try to find correlated columns
          require("dplyr")
          require("reshape2")
          fourier_d_cor <- as.matrix(cor(cur_xreg_fourier))
          fourier_d_cor_melt <-
            arrange(melt(fourier_d_cor),-abs(value))
          attach(fourier_d_cor_melt)
          fourier_cor_var_df <-
            fourier_d_cor_melt[which(Var1 != Var2 & value > 0.5), ]
          detach(fourier_d_cor_melt)
          if (nrow(fourier_cor_var_df)) {
            error(logger, sprintf("Fourier term xreg cor var: %s", as.character(format(
              head(fourier_cor_var_df)
            ))))
            write.table(head(fourier_cor_var_df),
                        file = logger$logfile,
                        row.names = FALSE)
          }
          
          require("usdm")
          v2 <- vifstep(cur_xreg_fourier, th = 10)
          sel_colnames <- as.character(v2@results$Variables)
          excluded_colnames <- as.character(v2@excluded)
          cur_filtered_xreg <- cur_xreg_fourier[, sel_colnames]
          if (nchar(excluded_colnames)){
            info(logger, sprintf(
              "predictors '%s' is excluded by VIF",
              paste(excluded_colnames, collapse = ",")
            ))
          }
          cur_xreg_fourier <- cur_filtered_xreg
          cur_fxreg_fourier <- cur_fxreg_fourier[, sel_colnames]
          if (is.rank.deficient(cur_xreg_fourier)){
            cur_xreg_fourier_valid <- FALSE
          }
        }
      } # cur_xerg_fourier_valid
      
      if (cur_bc_xreg_fourier_valid) {
        if (train_size <= ncol(cur_bc_xreg_fourier)) {
          # too few data for coefficients estimation
          cur_bc_xreg_fourier_valid <- FALSE
        }
      }
      if (cur_bc_xreg_fourier_valid){
        if (cur_holiday_valid &&
            ncol(cur_bc_xreg_fourier) + 1 < train_size) {
          cur_bc_xreg_fourier <- cbind(cur_bc_xreg_fourier, xreg_holiday)
          cur_bc_fxreg_fourier <-
            cbind(cur_bc_fxreg_fourier, fxreg_holiday)
        }
        if (ncol(cur_bc_xreg_fourier) + 1 < train_size) {
          cur_bc_xreg_fourier <- cbind(cur_bc_xreg_fourier, xreg_day)
          cur_bc_fxreg_fourier <-
            cbind(cur_bc_fxreg_fourier, fxreg_day)
        }
        dup_colnames <- duplicated(as.matrix(cur_bc_xreg_fourier), MARGIN = 2)
        stopifnot(!any(dup_colnames))
        bc_fourier_uniq_colnames <-
          colnames(unique(as.matrix(cur_bc_xreg_fourier), MARGIN = 2))
        if (length(bc_fourier_uniq_colnames)) {
          # remove duplicated columns
          cur_bc_xreg_fourier <- cur_bc_xreg_fourier[,bc_fourier_uniq_colnames]
          cur_bc_fxreg_fourier <-
            cur_bc_fxreg_fourier[,bc_fourier_uniq_colnames]
        }
        cur_bc_xreg_fourier_rank <- qr(cur_bc_xreg_fourier)$rank
        cur_bc_fxreg_fourier_rank <- qr(cur_bc_fxreg_fourier)$rank
        if (is.rank.deficient(cur_bc_xreg_fourier)) {
          # try to find correlated columns
          require("dplyr")
          require("reshape2")
          bc_fourier_d_cor <- as.matrix(cor(cur_bc_xreg_fourier))
          bc_fourier_d_cor_melt <- arrange(melt(bc_fourier_d_cor), -abs(value))
          attach(bc_fourier_d_cor_melt)
          bc_fourier_cor_var_df <-
            bc_fourier_d_cor_melt[which(Var1 != Var2 & value > 0.5),]
          detach(bc_fourier_d_cor_melt)
          if (nrow(bc_fourier_cor_var_df)) {
            # error(logger, sprintf("BoxCox transformed Fourier terms: %s", format(head(
            #   bc_fourier_cor_var_df
            # ))))
            write.table(head(bc_fourier_cor_var_df),
                        file = logger$logfile,
                        row.names = FALSE)
          }
          require("usdm")
          v2 <- vifstep(cur_bc_xreg_fourier, th = 10)
          sel_colnames <- as.character(v2@results$Variables)
          excluded_colnames <- as.character(v2@excluded)
          cur_filtered_xreg <- cur_bc_xreg_fourier[, sel_colnames]
          if (nchar(excluded_colnames)){
            info(logger, sprintf(
              "predictors '%s' is excluded by VIF",
              paste(excluded_colnames, collapse = ",")
            ))
          }
          cur_bc_xreg_fourier <- cur_filtered_xreg
          cur_bc_fxreg_fourier <- cur_bc_fxreg_fourier[, sel_colnames]
          if (is.rank.deficient(cur_bc_xreg_fourier)){
            cur_bc_xreg_fourier_valid <- FALSE
          }
        }
        
        # if (cur_fxreg_rank < ncol(xreg)) { # not necessary
        #   # rank deficient
        #   require("dplyr")
        #   require("reshape2")
        #   d_cor <- as.matrix(cor(fxreg))
        #   d_cor_melt <- arrange(melt(d_cor), -abs(value))
        #   attach(d_cor_melt)
        #   cor_var_df <-
        #     d_cor_melt[which(Var1 != Var2 & value > 0.5),]
        #   detach(d_cor_melt)
        #   error(logger, format(cor_var_df))
        #   cur_xreg_valid <<- FALSE
        # }
      } # cur_bc_xreg_fourier_valid
      
      if (cur_xreg_valid) {
        cur_fit_na <- FALSE
        cur_train_sample <- train_sample_ts
        cur_test_sample <- test_vv_count_ts
        cur_real_sample <- actual_vv_count_ts
        fit.start.time <- Sys.time()
        cur_xreg <- xreg
        cur_fxreg <- fxreg
        tryCatch({
          cur_fit <- auto.arima( # slow as hell due to many outliers when multiple years VV present compared with BoxCox transformed smooth version
            cur_train_sample,
            max.p = max_p,
            max.q = max_q,
            max.d = max_d,
            max.D = max_D,
            max.order = max_order,
            xreg = cur_xreg,
            approximation = FALSE,
            stepwise = FALSE,
            seasonal = TRUE,
            trace = FALSE
            # method = "ML" # avoid 'non-stationary AR part from CSS' error # No ARIMA model able to be estimated
            # trace = FALSE, # trace = TRUE,
            # parallel = TRUE #  2 nodes produced errors; first error: object 'max_order' not found
          )
        }, error = function(e) {
          error(logger, as.character(e))
          sink(cur_err_out)
          print(as.character(e))
          print(sys.calls())
          sink()
          cur_fit_na <<- TRUE
        }, finally = {
          
        })
        fit.end.time <- Sys.time()
        fit.time.taken <- fit.end.time - fit.start.time
        if (!cur_fit_na) {
          cur_fcast <-
            forecast(
              cur_fit,
              xreg = cur_fxreg,
              h = predict_ts_len,
              level = c(80, 95)
            )
          cur_fcast_value <- cur_fcast$mean
          cur_first_day_ape <-
            mape(as.numeric(cur_test_sample[1]),
                 as.numeric(cur_fcast_value[1]))
          cur_mape <- mape(as.numeric(cur_test_sample),
                           as.numeric(cur_fcast_value))
          cur_model_name <- as.character(cur_fit)
          cur_forecast_title <- sprintf(
            "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
            cur_bid_name,
            cur_pid_name,
            train_size,
            cur_model_name,
            cur_mape,
            cur_first_day_ape
          )
          
          first_day_ape_list <-
            c(first_day_ape_list, cur_first_day_ape)
          mape_list  <- c(mape_list, cur_mape)
          model_list <- c(model_list, list(cur_fit))
          forecast_list <- c(forecast_list, list(cur_fcast))
          model_name_list <- c(model_name_list, cur_model_name)
          forecast_title_list <-
            c(forecast_title_list, cur_forecast_title)
          plot_forecast_error_data_list <-
            c(plot_forecast_error_data_list, list(data.frame(
              pred = as.numeric(cur_fcast$mean), date = predict_date
            )))
          forecast_actual_data_list <-
            c(forecast_actual_data_list,
              list(cur_real_sample))
          # record relative importance of exogenous predictor variables
          cur_xreg_lm_data <- cbind(cur_train_sample, cur_xreg)
          cur_xreg_formula <-
            as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          cur_xreg_lm_fit <-
            lm(cur_xreg_formula, data = cur_xreg_lm_data)
          cur_xreg_lm_fit_residuals <- residuals(cur_xreg_lm_fit)
          cur_xreg_fitted_mape <-
            mape(cur_train_sample, fitted(cur_xreg_lm_fit))
          xreg_fitted_mape_list <-
            c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
          cur_rela_impo_stat <-
            sort(abs(lm.beta(cur_xreg_lm_fit)), decreasing = T) # standarized coefficients
          
          # require("relaimpo")
          # cur_xreg_lm_data <- cbind(cur_train_sample, cur_xreg)
          # cur_xreg_formula <- as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          # cur_xreg_lm_fit <- lm(cur_xreg_formula, data = cur_xreg_lm_data)
          # relimp.start.time <- Sys.time()
          # cur_rela_impo_stat <- calc.relimp(cur_xreg_lm_fit, type = c("lmg"), rela = TRUE) # slower as hell when dim is large
          # relimp.end.time <- Sys.time()
          # relimp.time.taken <- relimp.end.time - relimp.start.time
          # info(
          #   logger,
          #   sprintf(
          #     "'%s' xreg relative importance calculation time taken: %s",
          #     cur_model_name,
          #     format(relimp.time.taken)
          #   )
          # )
          
          info(
            logger,
            sprintf(
              "%s '%s' %s 耗时：%s, xreg_fitted_mape=%.4f, mape=%.4f, 首日预测误差%.4f\n\t%d predictors: %s",
              cur_bid_name,
              cur_pid_name,
              as.character(cur_fit),
              format(fit.time.taken),
              cur_xreg_fitted_mape,
              cur_mape,
              cur_first_day_ape,
              length(colnames(cur_xreg)),
              paste(colnames(cur_xreg), collapse = ",")
            )
          )
          
          png(
            sprintf("%s.png", xreg_lm_png_file_path_prefix),
            width = 1920,
            height = 1080
          )
          op <- par(mfrow = c(2, 2))
          plot(cur_xreg_lm_fit)
          par(op)
          dev.off()
          sink(xreg_stat_file_path)
          print(cur_rela_impo_stat)
          
          # print(cur_rela_impo_stat$lmg)
          # print("\n")
          # print(cur_rela_impo_stat$lmg.rank)
          sink()
        } # cur_fit_na
      } # cur_xreg_valid
     
      if (cur_bc_xreg_valid){
        cur_fit_na <- FALSE
        cur_train_sample <- train_sample_ts
        cur_test_sample <- test_vv_count_ts
        cur_real_sample <- actual_vv_count_ts
        cur_xreg <- bc_xreg
        cur_fxreg <- bc_fxreg
        fit.start.time <- Sys.time()
        tryCatch({
          # this error happen once in a while
          
          # Error in search.arima(x, d, D, max.p, max.q, max.P, max.Q, max.order,  :
          # No ARIMA model able to be estimated
          # https://github.com/robjhyndman/forecast/blob/master/R/arima.R
          cur_fit <- auto.arima(
            cur_train_sample,
            max.p = max_p,
            max.q = max_q,
            max.d = max_d,
            max.D = max_D,
            max.order = max_order,
            xreg = cur_xreg,
            lambda = lambda,
            approximation = FALSE,
            stepwise = FALSE,
            seasonal = TRUE,
            trace = FALSE
            # method = "ML"
            # trace = FALSE,
            # # trace = TRUE
            # parallel = TRUE
          )
        }, error = function(e) {
          error(logger, as.character(e))
          sink(cur_err_out)
          print(as.character(e))
          print(sys.calls())
          sink()
          cur_fit_na <<- TRUE
        }
        , finally = {
          
        })
        fit.end.time <- Sys.time()
        fit.time.taken <- fit.end.time - fit.start.time
        if (!cur_fit_na) {
          cur_fcast <-
            forecast(
              cur_fit,
              xreg = cur_fxreg,
              lambda = lambda,
              h = predict_ts_len,
              level = c(80, 95)
            )
          # cur_orig_fcast <-
          #   InvBoxCox(cur_fcast$mean, lambda = lambda)
          cur_fcast_value <- cur_fcast$mean
          cur_first_day_ape <-
            mape(as.numeric(cur_test_sample[1]),
                 as.numeric(cur_fcast_value[1]))
          cur_model_name <- as.character(cur_fit)
          cur_model_name <-
            sprintf("BoxCox-transformed(lambda=%.2f) %s",
                    lambda,
                    cur_model_name)
          cur_mape <- mape(as.numeric(cur_test_sample),
                           as.numeric(cur_fcast_value))
          cur_forecast_title <- sprintf(
            "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
            cur_bid_name,
            cur_pid_name,
            train_size,
            cur_model_name,
            cur_mape,
            cur_first_day_ape
          )
          first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
          mape_list = c(mape_list, cur_mape)
          model_list <- c(model_list, list(cur_fit))
          forecast_list <- c(forecast_list, list(cur_fcast))
          model_name_list <- c(model_name_list, cur_model_name)
          plot_forecast_error_data_list <-
            c(plot_forecast_error_data_list, list(data.frame(
              pred = as.numeric(cur_fcast_value), date = predict_date
            )))
          forecast_title_list <-
            c(forecast_title_list, cur_forecast_title)
          forecast_actual_data_list <-
            c(forecast_actual_data_list,
              list(cur_real_sample))
          # record relative importance of exogenous predictor variables
          cur_xreg_lm_data <- cbind(cur_train_sample, cur_xreg)
          cur_xreg_formula <-
            as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          cur_xreg_lm_fit <-
            lm(cur_xreg_formula, data = cur_xreg_lm_data)
          cur_xreg_fitted_mape <-
            mape(cur_train_sample, fitted(cur_xreg_lm_fit))
          xreg_fitted_mape_list <-
            c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
          cur_xreg_lm_fit_residuals <- residuals(cur_xreg_lm_fit)
          
          info(
            logger,
            sprintf(
              "%s '%s' %s 耗时：%s, xreg_fitted_mape=%.4f, mape=%.4f, 首日预测误差%.4f\n\t%d predictors: %s",
              cur_bid_name,
              cur_pid_name,
              cur_model_name,
              format(fit.time.taken),
              cur_xreg_fitted_mape,
              cur_mape,
              cur_first_day_ape,
              length(colnames(cur_xreg)),
              paste(colnames(cur_xreg), collapse = ",")
            )
          )
          
          png(
            sprintf("%s.png", bc_xreg_lm_png_file_path_prefix),
            width = 1920,
            height = 1080
          )
          op <- par(mfrow = c(2, 2))
          plot(cur_xreg_lm_fit)
          par(op)
          dev.off()
          cur_rela_impo_stat <-
            sort(abs(lm.beta(cur_xreg_lm_fit)), decreasing = T)
          
          # require("relaimpo")
          # cur_xreg_lm_data <- cbind(cur_train_sample, xreg)
          # cur_xreg_formula <- as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          # cur_xreg_lm_fit <- lm(cur_xreg_formula, data = cur_xreg_lm_data)
          # relimp.time.start <- Sys.time()
          # cur_rela_impo_stat <- calc.relimp(cur_xreg_lm_fit, type = c("lmg"), rela = TRUE)
          # relimp.time.taken <- relimp.end.time - relimp.start.time
          # info(
          #   logger,
          #   sprintf(
          #     "%s xreg relative importance calculation time taken: %s",
          #     cur_model_name,
          #     format(relimp.time.taken)
          #   )
          # )
          sink(bc_xreg_stat_file_path)
          print(cur_rela_impo_stat)
          
          # print(cur_rela_impo_stat$lmg)
          # print("\n")
          # print(cur_rela_impo_stat$lmg.rank)
          sink()
        } # !cur_fit_na
      } # if (cur_bc_xreg_valid)
      
      if (cur_xreg_fourier_valid) {
        # take FOREVER to finish with seasonal arima
        cur_fit_na <- FALSE
        cur_train_sample <- train_weekly_cnt_ts
        cur_test_sample <- test_weekly_cnt_ts
        cur_real_sample <- weekly_ts
        fit.start.time <- Sys.time()
        cur_xreg <- cur_xreg_fourier
        cur_fxreg <- cur_fxreg_fourier
        tryCatch({ # time consuming when multiple years daily VV is present
          cur_fit <- auto.arima(
            cur_train_sample,
            max.p = max_p,
            max.q = max_q,
            max.d = max_d,
            max.D = max_D,
            max.order = max_order,
            xreg = cur_xreg,
            lambda = NULL,
            approximation = FALSE,
            stepwise = FALSE,
            seasonal = FALSE,
            trace = FALSE
            # method = "ML" # avoid 'non-stationary AR part from CSS' error # No ARIMA model able to be estimated
            # trace = FALSE, # trace = TRUE,
            # parallel = TRUE #  2 nodes produced errors; first error: object 'max_order' not found
          )
        }, error = function(e) {
          error(logger, as.character(e))
          sink(cur_err_out)
          print(as.character(e))
          print(sys.calls())
          sink()
          cur_fit_na <<- TRUE
        }, finally = {

        })
        fit.end.time <- Sys.time()
        fit.time.taken <- fit.end.time - fit.start.time
        if (!cur_fit_na) {
          cur_fcast <-
            forecast(cur_fit,
                     xreg = cur_fxreg,
                     lambda = NULL,
                     h = predict_ts_len,
                     level = c(80, 95))
          cur_fcast_value <- cur_fcast$mean
          cur_first_day_ape <-
            mape(as.numeric(cur_test_sample[1]),
                 as.numeric(cur_fcast_value[1]))
          cur_mape <- mape(as.numeric(cur_test_sample),
                           as.numeric(cur_fcast_value))
          cur_model_name <- as.character(cur_fit)
          cur_model_name <- sprintf("%s using Fourier terms etc.", cur_model_name)
          cur_forecast_title <- sprintf(
            "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
            cur_bid_name,
            cur_pid_name,
            train_size,
            cur_model_name,
            cur_mape,
            cur_first_day_ape
          )

          first_day_ape_list <-
            c(first_day_ape_list, cur_first_day_ape)
          mape_list  <- c(mape_list, cur_mape)
          model_list <- c(model_list, list(cur_fit))
          forecast_list <- c(forecast_list, list(cur_fcast))
          model_name_list <- c(model_name_list, cur_model_name)
          forecast_title_list <-
            c(forecast_title_list, cur_forecast_title)
          plot_forecast_error_data_list <-
            c(plot_forecast_error_data_list, list(data.frame(
              pred = as.numeric(cur_fcast$mean), date = predict_date
            )))
          forecast_actual_data_list <-
            c(forecast_actual_data_list,
              list(cur_real_sample))
          # record partial relative importance of exogenous predictor variables
          cur_xreg_lm_data <-
            cbind(cur_train_sample, cur_xreg)
          cur_xreg_formula <-
            as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          cur_xreg_lm_fit <-
            lm(cur_xreg_formula, data = cur_xreg_lm_data)
          cur_xreg_lm_fit_residuals <- residuals(cur_xreg_lm_fit)
          cur_xreg_fitted_mape <-
            mape(cur_train_sample, fitted(cur_xreg_lm_fit))
          xreg_fitted_mape_list <-
            c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
          cur_rela_impo_stat <-
            sort(abs(lm.beta(cur_xreg_lm_fit)), decreasing = T) # standarized coefficients

          # require("relaimpo")
          # cur_xreg_lm_data <- cbind(cur_train_sample, xreg)
          # cur_xreg_formula <- as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          # cur_xreg_lm_fit <- lm(cur_xreg_formula, data = cur_xreg_lm_data)
          # relimp.start.time <- Sys.time()
          # cur_rela_impo_stat <- calc.relimp(cur_xreg_lm_fit, type = c("lmg"), rela = TRUE) # slower as hell when dim is large
          # relimp.end.time <- Sys.time()
          # relimp.time.taken <- relimp.end.time - relimp.start.time
          # info(
          #   logger,
          #   sprintf(
          #     "'%s' xreg relative importance calculation time taken: %s",
          #     cur_model_name,
          #     format(relimp.time.taken)
          #   )
          # )

          info(
            logger,
            sprintf(
              "%s '%s' %s 耗时：%s, xreg_fitted_mape=%.4f, mape=%.4f, 首日预测误差%.4f\n\t%d predictors: %s",
              cur_bid_name,
              cur_pid_name,
              cur_model_name,
              format(fit.time.taken),
              cur_xreg_fitted_mape,
              cur_mape,
              cur_first_day_ape,
              length(colnames(cur_xreg)),
              paste(colnames(cur_xreg), collapse = ",")
            )
          )

          png(
            sprintf("%s.png", fourier_xreg_lm_png_file_path_prefix),
            width = 1920,
            height = 1080
          )
          op <- par(mfrow = c(2, 2))
          plot(cur_xreg_lm_fit)
          par(op)
          dev.off()
        }

      } # if (cur_xreg_fourier_valid)
      
      if (cur_bc_xreg_fourier_valid){
        cur_fit_na <- FALSE
        cur_train_sample <- train_weekly_cnt_ts
        cur_test_sample <- test_weekly_cnt_ts
        cur_real_sample <- weekly_ts
        cur_xreg <- cur_bc_xreg_fourier
        cur_fxreg <- cur_bc_fxreg_fourier
        fit.start.time <- Sys.time()
        tryCatch({
          # this error happen once in a while
          
          # Error in search.arima(x, d, D, max.p, max.q, max.P, max.Q, max.order,  :
          # No ARIMA model able to be estimated
          # https://github.com/robjhyndman/forecast/blob/master/R/arima.R
          cur_fit <- auto.arima(
            cur_train_sample,
            max.p = max_p,
            max.q = max_q,
            max.d = max_d,
            max.D = max_D,
            max.order = max_order,
            xreg = cur_xreg,
            lambda = lambda,
            approximation = FALSE,
            stepwise = FALSE,
            seasonal = FALSE,
            trace = FALSE
            # method = "ML"
            # trace = FALSE,
            # # trace = TRUE
            # parallel = TRUE
          )
        }, error = function(e) {
          error(logger, as.character(e))
          sink(cur_err_out)
          print(as.character(e))
          print(sys.calls())
          sink()
          cur_fit_na <<- TRUE
        }
        , finally = {
          
        })
        fit.end.time <- Sys.time()
        fit.time.taken <- fit.end.time - fit.start.time
        if (!cur_fit_na) {
          cur_fcast <-
            forecast(cur_fit,
                     xreg = cur_bc_fxreg_fourier,
                     lambda = lambda,
                     h = predict_ts_len,
                     level = c(80, 95))
          cur_fcast_value <- cur_fcast$mean
          cur_first_day_ape <-
            mape(as.numeric(cur_test_sample[1]),
                 as.numeric(cur_fcast_value[1]))
          cur_model_name <- as.character(cur_fit)
          cur_model_name <-
            sprintf("BoxCox-transformed(lambda=%.2f) %s using Fourier terms etc.",
                    lambda,
                    cur_model_name)
          cur_forecast_title <- sprintf(
            "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
            cur_bid_name,
            cur_pid_name,
            train_size,
            cur_model_name,
            cur_mape,
            cur_first_day_ape
          )
          cur_mape <- mape(as.numeric(cur_test_sample),
                           as.numeric(cur_fcast_value))
          
          first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
          mape_list = c(mape_list, cur_mape)
          model_list <- c(model_list, list(cur_fit))
          forecast_list <- c(forecast_list, list(cur_fcast))
          model_name_list <- c(model_name_list, cur_model_name)
          plot_forecast_error_data_list <-
            c(plot_forecast_error_data_list, list(data.frame(
              pred = as.numeric(cur_fcast_value), date = predict_date
            )))
          forecast_title_list <-
            c(forecast_title_list, cur_forecast_title)
          forecast_actual_data_list <-
            c(forecast_actual_data_list,
              list(cur_real_sample))
          # record relative importance of partial exogenous predictor variables
          cur_xreg_lm_data <-
            cbind(cur_train_sample, cur_xreg)
          cur_xreg_formula <-
            as.formula("cur_xreg_lm_data[,1] ~ cur_xreg_lm_data[, 2:ncol(cur_xreg_lm_data)]")
          cur_xreg_lm_fit <-
            lm(cur_xreg_formula, data = cur_xreg_lm_data)
          cur_xreg_fitted_mape <-
            mape(cur_train_sample, fitted(cur_xreg_lm_fit))
          xreg_fitted_mape_list <-
            c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
          cur_xreg_lm_fit_residuals <- residuals(cur_xreg_lm_fit)
          
          cur_xreg_colnames <- colnames(cur_xreg)
          cur_xreg_colnames_str <- paste(cur_xreg_colnames, collapse = ",")
          info(
            logger,
            sprintf(
              "%s '%s' %s 耗时：%s, xreg_fitted_mape=%.4f, mape=%.4f, 首日预测误差%.4f\n\t%d predictors: %s",
              cur_bid_name,
              cur_pid_name,
              cur_model_name,
              format(fit.time.taken),
              cur_xreg_fitted_mape,
              cur_mape,
              cur_first_day_ape,
              length(cur_xreg_colnames),
              cur_xreg_colnames_str
            )
          )
          
          png(
            sprintf("%s.png", bc_fourier_xreg_lm_png_file_path_prefix),
            width = 1920,
            height = 1080
          )
          op <- par(mfrow = c(2, 2))
          plot(cur_xreg_lm_fit)
          par(op)
          dev.off()
          
        } # !cur_fit_na
      } # cur_bc_xreg_fourier_valid
    } # if using_xreg
    
    cur_fit_na <- FALSE
    train_x_new <- msts(train_sample_ts, seasonal.periods = c(7, 7 * 2))
    test_x_new <- msts(test_vv_count_ts, seasonal.periods = c(7, 7 * 2))
    actual_x_new <- msts(actual_vv_count_ts, seasonal.periods = c(7, 7 * 2))
    cur_train_sample <- train_x_new
    cur_test_sample <- test_x_new
    cur_real_sample <- actual_x_new
    cur_xreg_fitted_mape <- NA
    fit.start.time <- Sys.time()
    cur_fit <- tbats(cur_train_sample) # https://robjhyndman.com/hyndsight/forecasting-weekly-data/
    # https://robjhyndman.com/publications/complex-seasonality/
    fit.end.time <- Sys.time()
    fit.time.taken <- fit.end.time - fit.start.time
    cur_fcast <- forecast(cur_fit, h = predict_ts_len, level = c(80, 95))
    cur_fcast_value <- cur_fcast$mean
    cur_first_day_ape <-
      mape(as.numeric(cur_test_sample[1]),
           as.numeric(cur_fcast_value[1]))
    cur_mape <- mape(as.numeric(cur_test_sample),
                     as.numeric(cur_fcast_value))
    
    cur_model_name <- as.character(cur_fit)
    cur_forecast_title <- sprintf(
      "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
      cur_bid_name,
      cur_pid_name,
      train_size,
      cur_model_name,
      cur_mape,
      cur_first_day_ape
    )
    
    info(
      logger,
      sprintf(
        "%s '%s' %d points, '%s'耗时：%s, mape=%.4f, 首日预测误差%.4f",
        cur_bid_name,
        cur_pid_name,
        train_size,
        cur_model_name,
        format(fit.time.taken),
        cur_mape,
        cur_first_day_ape
      )
    )
    first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
    mape_list = c(mape_list, cur_mape)
    model_list <- c(model_list, list(cur_fit))
    forecast_list <- c(forecast_list, list(cur_fcast))
    model_name_list <- c(model_name_list, cur_model_name)
    plot_forecast_error_data_list <-
      c(plot_forecast_error_data_list, list(data.frame(
        pred = as.numeric(cur_fcast$mean), date = predict_date
      )))
    forecast_title_list <-
      c(forecast_title_list, cur_forecast_title)
    forecast_actual_data_list <-
      c(forecast_actual_data_list, list(cur_real_sample))
    xreg_fitted_mape_list <-
      c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
    
    
    
    cur_fit_na <- FALSE
    cur_train_sample <- train_sample_ts
    cur_test_sample <- test_vv_count_ts
    cur_real_sample <- actual_vv_count_ts
    cur_xreg_fitted_mape <- NA
    fit.start.time <- Sys.time()
    cur_fit <- ets(cur_train_sample) # https://robjhyndman.com/hyndsight/forecasting-weekly-data/
    # https://robjhyndman.com/publications/complex-seasonality/
    fit.end.time <- Sys.time()
    fit.time.taken <- fit.end.time - fit.start.time
    cur_fcast <- forecast(cur_fit, h = predict_ts_len, level = c(80, 95))
    cur_fcast_value <- cur_fcast$mean
    cur_first_day_ape <-
      mape(as.numeric(cur_test_sample[1]),
           as.numeric(cur_fcast_value[1]))
    cur_mape <- mape(as.numeric(cur_test_sample),
                     as.numeric(cur_fcast_value))
    
    cur_model_name <- as.character(cur_fit)
    cur_forecast_title <- sprintf(
      "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
      cur_bid_name,
      cur_pid_name,
      train_size,
      cur_model_name,
      cur_mape,
      cur_first_day_ape
    )
    
    info(
      logger,
      sprintf(
        "%s '%s' %d points, '%s'耗时：%s, mape=%.4f, 首日预测误差%.4f",
        cur_bid_name,
        cur_pid_name,
        train_size,
        cur_model_name,
        format(fit.time.taken),
        cur_mape,
        cur_first_day_ape
      )
    )
    first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
    mape_list = c(mape_list, cur_mape)
    model_list <- c(model_list, list(cur_fit))
    forecast_list <- c(forecast_list, list(cur_fcast))
    model_name_list <- c(model_name_list, cur_model_name)
    plot_forecast_error_data_list <-
      c(plot_forecast_error_data_list, list(data.frame(
        pred = as.numeric(cur_fcast$mean), date = predict_date
      )))
    forecast_title_list <-
      c(forecast_title_list, cur_forecast_title)
    forecast_actual_data_list <-
      c(forecast_actual_data_list, list(cur_real_sample))
    xreg_fitted_mape_list <-
      c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
    
    
    cur_fit_na <- FALSE
    cur_train_sample <- train_sample_ts
    cur_test_sample <- test_vv_count_ts
    cur_real_sample <- actual_vv_count_ts
    cur_xreg_fitted_mape <- NA
    fit.start.time <- Sys.time()
    cur_fit <- baggedETS(cur_train_sample) # https://robjhyndman.com/hyndsight/forecasting-weekly-data/
    # https://robjhyndman.com/publications/complex-seasonality/
    fit.end.time <- Sys.time()
    fit.time.taken <- fit.end.time - fit.start.time
    cur_fcast <- forecast(cur_fit, h = predict_ts_len, level = c(80, 95))
    cur_fcast_value <- cur_fcast$mean
    cur_first_day_ape <-
      mape(as.numeric(cur_test_sample[1]),
           as.numeric(cur_fcast_value[1]))
    cur_mape <- mape(as.numeric(cur_test_sample),
                     as.numeric(cur_fcast_value))
    
    # cur_model_name <- as.character(cur_fit) # not supported by baggedETS
    cur_model_name <- "baggedETS"
    cur_forecast_title <- sprintf(
      "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
      cur_bid_name,
      cur_pid_name,
      train_size,
      cur_model_name,
      cur_mape,
      cur_first_day_ape
    )
    
    info(
      logger,
      sprintf(
        "%s '%s' %d points, '%s'耗时：%s, mape=%.4f, 首日预测误差%.4f",
        cur_bid_name,
        cur_pid_name,
        train_size,
        cur_model_name,
        format(fit.time.taken),
        cur_mape,
        cur_first_day_ape
      )
    )
    first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
    mape_list = c(mape_list, cur_mape)
    model_list <- c(model_list, list(cur_fit))
    forecast_list <- c(forecast_list, list(cur_fcast))
    model_name_list <- c(model_name_list, cur_model_name)
    plot_forecast_error_data_list <-
      c(plot_forecast_error_data_list, list(data.frame(
        pred = as.numeric(cur_fcast$mean), date = predict_date
      )))
    forecast_title_list <-
      c(forecast_title_list, cur_forecast_title)
    forecast_actual_data_list <-
      c(forecast_actual_data_list, list(cur_real_sample))
    xreg_fitted_mape_list <-
      c(xreg_fitted_mape_list, cur_xreg_fitted_mape)

    
    cur_fit_na <- FALSE
    cur_train_sample <- train_sample_ts
    cur_test_sample <- test_vv_count_ts
    cur_real_sample <- actual_vv_count_ts
    cur_xreg_fitted_mape <- NA
    fit.start.time <- Sys.time()
    tryCatch({
      cur_fit <- auto.arima(
        cur_train_sample,
        max.p = max_p,
        max.q = max_q,
        max.d = max_d,
        max.D = max_D,
        max.order = max_order,
        xreg = NULL,
        lambda = NULL,
        approximation = FALSE,
        stepwise = FALSE,
        seasonal = TRUE, # not effective if frequency is 1
        trace = FALSE
        # method = "ML"
        # trace = FALSE,
        # # trace = TRUE
        # parallel = TRUE
      )
    }, error = function(e) {
      error(logger, as.character(e))
      sink(cur_err_out)
      print(as.character(e))
      print(sys.calls())
      sink()
      cur_fit_na <<- TRUE
    }, finally = {
      
    })
    fit.end.time <- Sys.time()
    fit.time.taken <- fit.end.time - fit.start.time
    if (!cur_fit_na) {
      cur_fcast <-
        forecast(
          cur_fit,
          h = predict_ts_len,
          lambda = NULL,
          xreg = NULL,
          level = c(80, 95)
        )
      cur_fcast_value <- cur_fcast$mean
      cur_first_day_ape <- mape(as.numeric(cur_test_sample[1]),
                                as.numeric(cur_fcast_value[1]))
      cur_mape <- mape(as.numeric(cur_test_sample),
                       as.numeric(cur_fcast_value))
      cur_model_name <- as.character(cur_fit)
      cur_forecast_title <- sprintf(
        "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
        cur_bid_name,
        cur_pid_name,
        train_size,
        cur_model_name,
        cur_mape,
        cur_first_day_ape
      )
      
      info(
        logger,
        sprintf(
          "%s '%s' %d points, '%s'耗时：%s, mape=%.4f, 首日预测误差%.4f",
          cur_bid_name,
          cur_pid_name,
          train_size,
          cur_model_name,
          format(fit.time.taken),
          cur_mape,
          cur_first_day_ape
        )
      )
      first_day_ape_list = c(first_day_ape_list, cur_first_day_ape)
      mape_list = c(mape_list, cur_mape)
      model_list <- c(model_list, list(cur_fit))
      forecast_list <- c(forecast_list, list(cur_fcast))
      model_name_list <- c(model_name_list, cur_model_name)
      plot_forecast_error_data_list <-
        c(plot_forecast_error_data_list, list(data.frame(
          pred = as.numeric(cur_fcast$mean), date = predict_date
        )))
      forecast_title_list <-
        c(forecast_title_list, cur_forecast_title)
      forecast_actual_data_list <-
        c(forecast_actual_data_list, list(cur_real_sample))
      xreg_fitted_mape_list <- c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
    }
    
    cur_fit_na <- FALSE
    cur_train_sample <- train_sample_ts
    cur_test_sample <- test_vv_count_ts
    cur_real_sample <- actual_vv_count_ts
    cur_xreg_fitted_mape <- NA
    fit.start.time <- Sys.time()
    tryCatch({
      cur_fit <- auto.arima(
        cur_train_sample,
        max.p = max_p,
        max.q = max_q,
        max.d = max_d,
        max.D = max_D,
        max.order = max_order,
        xreg = NULL,
        lambda = lambda,
        approximation = FALSE,
        stepwise = FALSE,
        seasonal = TRUE,
        trace = FALSE
        # method = "ML"
        # trace = FALSE,
        # # trace = TRUE
        # parallel = TRUE
      )
    }, error = function(e) {
      error(logger, as.character(e))
      sink(cur_err_out)
      print(as.character(e))
      print(sys.calls())
      sink()
      cur_fit_na <<- TRUE
    }, finally = {
      
    })
    fit.end.time <- Sys.time()
    fit.time.taken <- fit.end.time - fit.start.time
    if (!cur_fit_na) {
      cur_fcast <-
        forecast(
          cur_fit,
          xreg = NULL,
          lambda = lambda,
          h = predict_ts_len,
          level = c(80, 95)
        )
      # cur_orig_fcast <- InvBoxCox(cur_fcast$mean, lambda = lambda)
      cur_fcast_value <- cur_fcast$mean
      cur_first_day_ape <-
        mape(as.numeric(cur_test_sample[1]),
             as.numeric(cur_fcast_value[1]))
      cur_mape <- mape(as.numeric(cur_test_sample),
                       as.numeric(cur_fcast_value))
      cur_model_name <- as.character(cur_fit)
      cur_model_name <-
        sprintf("BoxCox-transformed(lambda=%.2f) %s",
                lambda,
                cur_model_name)
      cur_forecast_title <- sprintf(
        "%s '%s' %d points, Forecasts from %s\nMean Absolute Percentage Error = %.4f, first day Percentage Error = %.4f",
        cur_bid_name,
        cur_pid_name,
        train_size,
        cur_model_name,
        cur_mape,
        cur_first_day_ape
      )
      
      info(
        logger,
        sprintf(
          "%s '%s' %d points, '%s'耗时：%s, mape=%.4f, 首日预测误差%.4f",
          cur_bid_name,
          cur_pid_name,
          train_size,
          cur_model_name,
          format(fit.time.taken),
          cur_mape,
          cur_first_day_ape
        )
      )
      first_day_ape_list <- c(first_day_ape_list, cur_first_day_ape)
      mape_list = c(mape_list, cur_mape)
      model_list <- c(model_list, list(cur_fit))
      forecast_list <- c(forecast_list, list(cur_fcast))
      model_name_list <- c(model_name_list, cur_model_name)
      plot_forecast_error_data_list <-
        c(plot_forecast_error_data_list, list(data.frame(
          pred = as.numeric(cur_fcast_value), date = predict_date
        )))
      forecast_title_list <-
        c(forecast_title_list, cur_forecast_title)
      forecast_actual_data_list <-
        c(forecast_actual_data_list,
          list(cur_real_sample))
      xreg_fitted_mape_list <- c(xreg_fitted_mape_list, cur_xreg_fitted_mape)
    }
    
    
    # # ploting $n$ model's prediction comparison
    sub_plot_num <- length(forecast_title_list)
    png(
      paste0(png_predict_file_path_prefix, "_forecast.png"),
      width = 1920,
      height = 720 * sub_plot_num
    )
    op <- par(mfrow = c(sub_plot_num, 1))
    require("lubridate")
    for (plot_idx in seq_along(forecast_title_list)) {
      cur_plot_model <- forecast_list[[plot_idx]]
      cur_plot_title <- forecast_title_list[[plot_idx]]
      cur_actual_ts <- forecast_actual_data_list[[plot_idx]]
      cur_a <-
        seq(start_date[[i]],
            by = "days",
            length = length(vv_count_ts))
      cur_a_tick <- seq(from = 1,
                        to = length(vv_count_ts),
                        by = 7)
      cur_a_tick_label <- format(cur_a, "%Y-%m-%d")[cur_a_tick]
      plot(cur_plot_model,
           # col = "magenta", # not working
           main = cur_plot_title,
           pch = 17,
           xaxt = "n")
      # https://stackoverflow.com/questions/10302261/forecasting-time-series-data#answer-10347205
      axis(
        1,
        at = cur_a_tick,
        labels = cur_a_tick_label,
        cex.axis = 1,
        las = 3
      )
      lines(fitted(cur_plot_model),
            type = c("b", "c"),
            pch = 18,
            col = "green")
      lines(cur_actual_ts,
            type = c("b", "c"),
            pch = 19,
            col = "red")
      legend(
        "bottomright",
        legend = c("forecast", "fitted", "real"),
        col = c("blue", "green", "red"),
        pch = c(17, 18, 19),
        bty = "n",
        pt.cex = 2,
        cex = 1.2,
        text.col = "black",
        horiz = F,
        inset = c(0.1, 0.1)
      )
      # actual_vv_count_ts_df <- fortify(actual_vv_count_ts)
      # p <-
      #   ggplot(data = actual_vv_count_ts_df, aes(x = Index, y = x)) + geom_line() + geom_point()
      # p <- p + scale_x_date(labels = date_format("%Y-%m-%d")) +
      #   theme(axis.text.x = element_text(
      #     angle = 90,
      #     vjust = 0.5,
      #     hjust = 1
      #   ))
      # p <- p + geom_l
    } # for every forecast sub-plot
    par(op)
    dev.off()
    
    rep_model_name <- c()
    new.df <- data.frame()
    rep_cnt <- c()
    for (pred_idx in seq_along(plot_forecast_error_data_list)) {
      plot_df <- plot_forecast_error_data_list[[pred_idx]]
      plot_model_name <- model_name_list[[pred_idx]]
      plot_df$pred_pe <-
        as.numeric(abs(plot_df$pred - test_vv_count_ts) / test_vv_count_ts)
      new.df <- rbind(new.df, plot_df)
      plot_model_title <-
        sprintf(
          "%s, mape=%.4f, first forecast PE=%.4f",
          plot_model_name,
          mape_list[[pred_idx]],
          first_day_ape_list[[pred_idx]]
        )
      rep_model_name <-
        c(rep_model_name, plot_model_title)
      rep_cnt <- c(rep_cnt, nrow(plot_df))
    }
    new.df$model_name <- rep(rep_model_name, rep_cnt)
    # ploting forecast Percentage Error comparison
    p <- ggplot() + scale_x_date("日期")
    p <-
      p + geom_line(data = new.df, aes(x = date,
                                       y = pred_pe,
                                       colour = model_name)) + geom_point() + ylab("预测误差比例")
    ggsave(
      paste0(png_file_path_prefix, "_forecast_comp.png"),
      width = 12,
      height = 6
    )
    
    residuals_list <-
      lapply(model_list, residuals)
    best_model_idx <- which.min(mape_list)
    choosen_model <- model_list[[best_model_idx]]
    choosen_model_df <- model.df(choosen_model)
    choosen_residuals <- residuals(choosen_model)
    # choosen_model_name <- as.character(choosen_model)
    choosen_model_name <- model_name_list[[best_model_idx]]
    choosen_mape <- mape_list[[best_model_idx]]
    choosen_first_day_ape <-
      first_day_ape_list[[best_model_idx]]
    choosen_forecast <- forecast_list[[best_model_idx]]
    choosen_model_accuracy <-
      forecast::accuracy(f = plot_forecast_error_data_list[[best_model_idx]]$pred, x = test_vv_count_ts)
    choosen_xreg_fitted_mape <- xreg_fitted_mape_list[[best_model_idx]]
    
    ## not working
    # choosen_residuals <-
    #   ifelse(cur_mape < trans_mape,
    #          residuals(fit_w_seasonality),
    #          residuals(trans_fit))
    names(choosen_residuals) <-
      paste(cur_pid_name, choosen_model_name)
    residuals_desc_str <-
      sprintf(
        "%s_%s-%s_%sdays-holdout\n%s residuals",
        cur_pid_name,
        cur_start_date_str,
        cur_pred_end_date_str,
        predict_ts_len,
        choosen_model_name
      )
    
    # various residuals test
    
    Pacf_plot <- FALSE
    acf_plot <- FALSE
    QQ_plot <- FALSE
    
    cur_box_test_fitdf <- model.df(choosen_model)
    # #ARIMA model only
    # if (is.element("Arima", class(choosen_model))){
    #   cur_box_test_fitdf <- length(choosen_model$coef)
    # 
    #   # model_paras <- choosen_model$arma
    #   # model_paras <-
    #   #   setNames(model_paras, c("p", "q", "P", "Q", "m", "d", "D"))
    #   # m <- floor(sqrt(length(choosen_model$fitted)))
    #   # stopifnot(m < length(choosen_model$fitted))
    #   # cur_box_test_fitdf <-
    #   #   model_paras["p"] + model_paras["q"]  # p + q
    #   # cur_box_test_lag <- m
    #   # if (m - cur_box_test_fitdf <= 0) {
    #   #   cur_box_test_lag <- 1 # defalut value
    #   #   cur_box_test_fitdf <- 0 # default value
    #   # }
    # } else if (is.element("bats", class(choosen_model))){
    #   cur_box_test_fitdf <- length(choosen_model$parameters$vect) + NROW(choosen_model$seed.states)
    # }
    cur_model_na <- FALSE
    if (!is.na(cur_box_test_fitdf) && cur_box_test_fitdf >= train_size) {
      warn(
        logger,
        sprintf(
          "%s '%s':\tunable to fit %d sample with degree of freedom %d of '%s'",
          cur_bid_name,
          cur_pid_name,
          train_size,
          cur_box_test_fitdf,
          choosen_model_name
        )
      )
      cur_model_na <- TRUE
    }
    
    acf_plot <- TRUE
    Pacf_plot <- TRUE
    QQ_plot <- TRUE
    cur_residuals_stationary <- TRUE
    adf_test_res <-
      adf.test(choosen_residuals, alternative = "stationary")
    if (adf_test_res$p.value > 0.05) {
      info(
        logger,
        sprintf(
          "'%s' fitted '%s' residuals have unit-root, hence it is non-stationary!!\nadf.test's p.value=%.4f",
          cur_pid_name,
          choosen_model_name,
          adf_test_res$p.value
        )
      )
      acf_plot <- TRUE
      Pacf_plot <- TRUE
      QQ_plot <- TRUE
      cur_residuals_stationary <- FALSE
    }
    
    Box_test_res <- NA
    cur_residuals_independent <- NA
    if (!is.na(cur_box_test_fitdf)){
      freq <- frequency(choosen_residuals)
      cur_box_test_lag <-
        max(cur_box_test_fitdf + 3, ifelse(freq > 1, 2 * freq, 10))
      cur_box_test_lag <-
        min(cur_box_test_lag, length(residuals) - 1L)
      Box_test_res <- Box.test(
        choosen_residuals,
        lag = cur_box_test_lag,
        type = c("Ljung-Box"),
        fitdf = cur_box_test_fitdf
      )
      cur_residuals_independent <- TRUE
      # https://stats.stackexchange.com/questions/148004/testing-for-autocorrelation-ljung-box-versus-breusch-godfrey#answer-148290
      if (!is.na(Box_test_res$p.value) &&
          Box_test_res$p.value < 0.05) {
        cur_residuals_variance <-
          as.character(choosen_model$model$sigma2)
        info(
          logger,
          sprintf(
            "'%s' '%s' residuals is NOT independent up to lag %d, hence it is NOT white noise! Residuals variance %s\nBox.test's p.value=%.4f",
            cur_pid_name,
            choosen_model_name,
            cur_box_test_lag,
            cur_residuals_variance,
            Box_test_res$p.value
          )
        )
        acf_plot <- TRUE
        Pacf_plot <- TRUE
        QQ_plot <- TRUE
        cur_residuals_independent <- FALSE
      }
    }
    
    if (!acf_plot) {
      png(
        paste0(png_file_path_prefix, "_acf.png"),
        width = 1920,
        height = 1080
      )
      acf(choosen_residuals, main = residuals_desc_str)
      dev.off()
      acf_plot <- TRUE
    }
    if (!Pacf_plot) {
      png(
        paste0(png_file_path_prefix, "_Pacf.png"),
        width = 1920,
        height = 1080
      )
      Pacf(choosen_residuals, main = residuals_desc_str)
      dev.off()
      Pacf_plot <- TRUE
    }
    if (!QQ_plot) {
      # visualize residuals normality
      png(
        paste0(png_file_path_prefix, "_residuals_Q-Q_Plot.png"),
        width = 1920,
        height = 1080
      )
      op <- par(mfrow = c(1, 2))
      qqnorm(choosen_residuals,
             main = paste(residuals_desc_str, "\n", "Normal Q-Q Plot"))
      qqline(choosen_residuals)
      
      h <- hist(choosen_residuals,
                main = sprintf("%s 拟合残差直方图",
                               residuals_desc_str))
      xfit <-
        seq(min(choosen_residuals),
            max(choosen_residuals),
            length = length(choosen_residuals))
      yfit <-
        dnorm(xfit,
              mean = mean(choosen_residuals),
              sd = sd(choosen_residuals))
      yfit <-
        yfit * diff(h$mids[1:2]) * length(choosen_residuals)
      lines(xfit, yfit, col = "black", lwd = 2)
      par(op)
      dev.off()
      QQ_plot <- TRUE
    }
    
    png(
      sprintf("%s_boxcox_comp.png", png_file_path_prefix),
      width = 1920,
      height = 1080
    )
    op <- par(mfrow = c(2, 2))
    qqnorm(train_sample_ts,
           main = sprintf("%s\nNormal Q-Q Plot", out_file_desc_prefix))
    qqline(train_sample_ts)
    
    h <- hist(train_sample_ts,
              main = sprintf("%s 每日VV直方图",
                             out_file_desc_prefix))
    xfit <-
      seq(
        min(train_sample_ts),
        max(train_sample_ts),
        length = length(train_sample_ts)
      )
    yfit <-
      dnorm(
        xfit,
        mean = mean(train_sample_ts),
        sd = sd(train_sample_ts)
      )
    yfit <-
      yfit * diff(h$mids[1:2]) * length(train_sample_ts)
    lines(xfit, yfit, col = "black", lwd = 2)
    
    qqnorm(
      bc_train_clean_cnt_sample_ts,
      main = sprintf(
        "%s\nbox-cox transfored Normal Q-Q Plot",
        out_file_desc_prefix
      )
    )
    qqline(bc_train_clean_cnt_sample_ts)
    
    h <-
      hist(
        bc_train_clean_cnt_sample_ts,
        main = sprintf(
          "%s 每日VV直方图\nbox-cox transformed, lambda=%.2f",
          out_file_desc_prefix,
          lambda
        )
      )
    xfit <-
      seq(
        min(bc_train_clean_cnt_sample_ts),
        max(bc_train_clean_cnt_sample_ts),
        length = length(bc_train_clean_cnt_sample_ts)
      )
    yfit <-
      dnorm(
        xfit,
        mean = mean(bc_train_clean_cnt_sample_ts),
        sd = sd(bc_train_clean_cnt_sample_ts)
      )
    yfit <-
      yfit * diff(h$mids[1:2]) * length(bc_train_clean_cnt_sample_ts)
    lines(xfit, yfit, col = "black", lwd = 2)
    par(op)
    dev.off()
    
    png(
      sprintf("%s_tsdisplay.png", png_file_path_prefix),
      width = 1920,
      height = 1080
    )
    tsdisplay(choosen_residuals,
              main = sprintf("%s_'%s' residuals", out_file_desc_prefix, choosen_model_name))
    dev.off()
    
    cur_test_mean_abs_vibr_ratio <-
      mean(abs(tail(
        cur_daily_data$clean_cnt_change_ratio, predict_ts_len
      )))
    cur_first_day_change_ratio <- cur_daily_data$clean_cnt_change_ratio[train_size + 1]
    cur_info_pack <-
      c(
        pid = cur_pid,
        pid_name = cur_pid_name,
        start_date_str = cur_start_date_str,
        predict_end_date_str = cur_pred_end_date_str,
        train_ts_length = train_size,
        predict_ts_length = predict_ts_len,
        model = list(choosen_model),
        model_desc = choosen_model_name,
        forecast = list(choosen_forecast),
        residuals = list(choosen_residuals),
        Box_test_res = list(Box_test_res),
        adf_test_res = list(adf_test_res),
        mape = choosen_mape,
        first_day_ape = choosen_first_day_ape,
        plot_forecast_error_data_list = plot_forecast_error_data_list,
        residuals_stationary = cur_residuals_stationary,
        residuals_independent = cur_residuals_independent,
        test_mean_abs_ratio = cur_test_mean_abs_vibr_ratio,
        first_day_change_ratio = cur_first_day_change_ratio,
        model_accuracy = choosen_model_accuracy,
        xreg = xreg,
        fxreg = fxreg,
        xreg_fitted_mape = choosen_xreg_fitted_mape,
        model_na = cur_model_na,
        model_df = choosen_model_df
      )
    model_info_list[[cur_bid_str]][[cur_pid]] = cur_info_pack
    
  } # for every pid
  
} # for every bid
# print model stat table
# print(model_info_list)
sql_bid_cond_str_list <- names(model_info_list)
for (bid_str_idx in seq_along(sql_bid_cond_str_list)) {
  bid_str <- sql_bid_cond_str_list[[bid_str_idx]]
  info(logger, bid_name_list[[bid_str_idx]])
  
  fileConn <- file(stat_file_list[[bid_str_idx]], 'w')
  write(
    sprintf(
      "pid\tpid_name\tstart_date\tlast_pred_date\ttrain_size\tMAPE\ttest_mean_abs_ratio\tfirst_day_ape\tfirst_day_change_ratio\txreg_fitted_mape\tmodel_desc\tresiduals_stationary\tresiduals_independent\tunable_to_fit\tmodel_para_num"
    ),
    fileConn,
    append = TRUE,
    sep = "\n"
  )
  
  for (pid_str in names(model_info_list[[bid_str]])) {
    cur_model_info <- model_info_list[[bid_str]][[pid_str]]
    out_line <- sprintf(
      "%s\t'%s'\t%s\t%s\t%d\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t'%s'\t%s\t%s\t%s\t%d",
      cur_model_info$pid,
      cur_model_info$pid_name,
      cur_model_info$start_date_str,
      cur_model_info$predict_end_date_str,
      cur_model_info$train_ts_length,
      cur_model_info$mape,
      cur_model_info$test_mean_abs_ratio,
      cur_model_info$first_day_ape,
      cur_model_info$first_day_change_ratio,
      cur_model_info$xreg_fitted_mape,
      cur_model_info$model_desc,
      cur_model_info$residuals_stationary,
      cur_model_info$residuals_independent,
      cur_model_info$model_na,
      cur_model_info$model_df
    )
    cat(paste0(out_line, "\n"))
    write(out_line, fileConn, append = TRUE, sep = "\n")
  }
  for (pid_idx in seq_along(skipped_pid_list[[bid_str]])) {
    pid <- skipped_pid_list[[bid_str]][[pid_idx]]
    pid_name <- skipped_pid_name_list[[bid_str]][[pid_idx]]
    write(
      sprintf(
        "%s\t'%s'\tNA\tNA\t%d\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA",
        pid,
        pid_name,
        skipped_pid_train_size_list[[bid_str]][[pid_idx]]
      ),
      fileConn,
      append = TRUE,
      sep =
        "\n"
    )
  }
  if (length(skipped_pid_name_list)) {
    cat(sprintf(
      "Skipped TV program: '%s'\n",
      paste(skipped_pid_name_list[[bid_str]], ",")
    ))
  }
  cat()
  close(fileConn)
}
mysql_dbDisconnectAll(mysql.drv)
postgresql_dbDisconnectAll(drv)
