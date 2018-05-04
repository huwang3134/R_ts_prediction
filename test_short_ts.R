# clean up mess from previous aborted run

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
top_pid_cnt <- 10

max_train_size <- 24 * 14
max_lag_allow <- 24

test_ts_list <- c()
test_xts_list <- c()
best_model_list <- c()
model_fit_time_list <- c()
# max_sample_size <- 120
# test_mean <- 100000
# test_sd <- test_mean * 0.3
y_freq <- 24
hold_out_h <- y_freq

require("log4r")
logger <- create.logger(logfile = "", level = "INFO")

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
  # dbClearResult(dbListResults(conn)[[1]])
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

yesterday <- Sys.Date() - 1
yesterday_str <- format(yesterday, "%Y%m%d")
vv_start_date <- yesterday - vv_stat_days_count + 1
vv_start_date_str <- format(vv_start_date, "%Y%m%d")
vv_end_date_str <- yesterday_str
last_feasible_day <- yesterday

# https://www.timeanddate.com/holidays/china/
holidays_df <-
  read.table(
    "~/R_BoxCox_arima_batch_prediction/chinese_holidays",
    sep = "\t",
    quote = "",
    header = TRUE
  )
holidays_df$Date <- as.Date(strptime(holidays_df$Date, "%Y %b %d"))

bid_str_list <- c("(9)", "(12)")  # aPhone, iPhone
bid_name_list <- c("aPhone", "iPhone")
skipped_pid_list <- c(c())
skipped_pid_name_list <- c(c())
forecast_list <-
  c() # forecast value list
rmse_list <-
  c() # CV RMSE value list
mape_list <-
  c() # CV MAPE value list
clip_info_list <- c()


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
  # pid_str_list <- c("317663", "321502")
  
  mysql.drv <- MySQL()
  mysql.db = dbConnect(
    mysql.drv,
    user = 'bigdata_r',
    password = 'B1gD20t0_r',
    port = 3306,
    host = '10.27.106.230',
    dbname = "mgmd"
  )
  
  sql <-
    sprintf(
      'select clipId, clipName, updateTime, releaseTime  from asset_v_clips where clipId in %s;',
      paste0('(', paste(pid_str_list, collapse = ","), ')')
    )
  print(sql)
  dbSendQuery(mysql.db, "SET NAMES utf8")
  clip_info_list[[cur_bid_str]] <- dbGetQuery(mysql.db, sql)
  clip_info_list[[cur_bid_str]]$updateTime <-
    strptime(clip_info_list[[cur_bid_str]]$updateTime, format = "%Y-%m-%d %H:%M:%S")
  clip_info_list[[cur_bid_str]]$releaseTime <-
    strptime(clip_info_list[[cur_bid_str]]$releaseTime, format = "%Y-%m-%d %H:%M:%S")
  
  start_date_str_list <-
    format(clip_info_list[[cur_bid_str]]$releaseTime, format = "%Y%m%d")
  start_date <- as.Date(clip_info_list[[cur_bid_str]]$releaseTime)
  end_date <- as.Date(clip_info_list[[cur_bid_str]]$updateTime)
  
  # predict_end_date <- end_date + hold_out_h
  if (is.null(max_train_size)) {
    predict_end_date <- end_date + hold_out_h
  } else {
    predict_end_date <-
      start_date + (max_train_size - 1) + hold_out_h
  }
  predict_end_date <-
    as.Date(unlist(lapply(predict_end_date, function(x)
      ifelse(x > last_feasible_day, last_feasible_day, x))))
  pred_end_date_str_list <-
    format(predict_end_date, format = "%Y%m%d")
  pid_name_list <- clip_info_list[[cur_bid_str]]$clipName
  pid_name_list <-
    gsub(" ", "", pid_name_list, fixed = TRUE)  # remove white spaces
  pid_str_list <- as.character(clip_info_list[[cur_bid_str]]$clipId)
  
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
    info(
      logger,
      sprintf(
        "%s 查询%d天逐小时VV耗时:%s",
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
    cur_hourly_vv_df$is_holiday <- cur_hourly_vv_df$Date %in% holidays_df$Date
    
    # test_hourly_vv_df <-
    #   read.table("pid-320519_20180131-20180206_hourly_VV", header = T)
    # test_hourly_vv_df$time <-
    #   strptime(test_hourly_vv_df$date, format = "%Y%m%d%H")
    
    cur_sample_size <- nrow(cur_hourly_vv_df)
    if (cur_sample_size <= 2 * y_freq) {
      WARN(
        logger,
        sprintf(
          "%s %d is less than %d, skipped",
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
    CV_cnt <- length(cur_train_h_seq)
    
    
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
      # if (cur_train_size > 2 * y_freq){
      #   seasonplot(cur_train_y_ts)
      #   var(stl(cur_train_y_ts, s.window = "periodic")$time.series[,"seasonal"]) / var(cur_train_y_ts)
      # }
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
      
      # train_fourier_list <- c()
      # test_fourier_list <- c()
      #
      fourier_K <- floor(24 / 2)
      start.time <- Sys.time()
      # https://github.com/robjhyndman/forecast/blob/8826e627a1d0e3cfa9cd8e01e17c912cfe40625c/R/season.R#L251
      train_daily_four_reg <-
        fourier(cur_train_y_ts, K = fourier_K) # return value is INDEPENDENT of ts value and only the length of ts will be considered by fourier function
      end.time <- Sys.time()
      train_four_reg <- train_daily_four_reg
      # train_fourier_list <-
      #   c(train_fourier_list, list(train_daily_four_reg))
      start.time <- Sys.time()
      time.taken <- start.time - end.time
      info(
        logger,
        sprintf(
          "%s daily fourier xreg fitting time taken %s",
          cur_out_comm_prefix,
          format(time.taken)
        )
      )
      test_daily_four_reg <-
        fourier(cur_train_y_ts, K = fourier_K, h = hold_out_h)
      end.time <- Sys.time()
      time.taken <- end.time - start.time
      info(
        logger,
        sprintf(
          "%s daily fourier future xreg fitting time taken %s",
          cur_out_comm_prefix,
          format(time.taken)
        )
      )
      test_four_reg <- test_daily_four_reg
      # test_fourier_list <-
      #   c(test_fourier_list, list(test_daily_four_reg))
      
      train_msts_four_reg <- NULL
      test_msts_four_reg <- NULL
      if (cur_train_size > 24 * 7) {
        if (cur_train_size > 24 * 365) {
          cur_train_y_msts <-
            msts(cur_train_y_ts,
                 seasonal.periods = c(24, 24 * 7, 24 * 365.25)) # daily, weekly, yearly seasonality
          # fourier_K <- c(12, 84, 4383)
          fourier_K <-
            c(floor(24 / 2), floor(24 * 7 / 2), floor(24 * 365.25 / 2)) # using higherst possible K might overfit when AICc is more approriate but the latter is time consuming
        } else {
          cur_train_y_msts <-
            msts(cur_train_y_ts, seasonal.periods = c(24, 24 * 7)) # daily, weekly seasonality
          fourier_K <- c(floor(24 / 2), floor(24 * 7 / 2))
        }
        start.time <- Sys.time()
        train_msts_four_reg <-
          fourier(cur_train_y_msts, K = fourier_K)
        end.time <- Sys.time()
        time.taken <- end.time - start.time
        info(
          logger,
          sprintf(
            "%s multiple seasonal fourier xreg fitting time taken %s",
            cur_out_comm_prefix,
            format(time.taken)
          )
        )
        start.time <- Sys.time()
        test_msts_four_reg <-
          fourier(cur_train_y_msts, K = fourier_K, h = hold_out_h)
        end.time <- Sys.time()
        time.taken <- end.time - start.time
        info(
          logger,
          sprintf(
            "%s multiple seasonal fourier future xreg fitting time taken %s",
            cur_out_comm_prefix,
            format(time.taken)
          )
        )
        
        train_four_reg <- train_msts_four_reg
        test_four_reg <- test_msts_four_reg
        
        # train_fourier_list <-
        #   c(train_fourier_list, list(train_weekly_four_reg))
        # start.time <- Sys.time()
        # test_weekly_four_reg <-
        #   fourier(cur_weekly_train_y_ts, K = 12, h = hold_out_h)
        # end.time <- Sys.time()
        # time.taken <- end.time - start.time
        # info(
        #   logger,
        #   sprintf(
        #     "%s weekly fourier future xreg fitting time taken %s",
        #     cur_out_comm_prefix,
        #     format(time.taken)
        #   )
        # )
        # test_fourier_list <-
        #   c(test_fourier_list, list(test_weekly_four_reg))
      }
      
      # train_four_reg <- NULL
      # for (cur_train_four_reg in train_fourier_list) {
      #   train_four_reg <- cbind(train_four_reg, cur_train_four_reg)
      # }
      # test_four_reg <- NULL
      # for (cur_test_four_reg in test_fourier_list) {
      #   test_four_reg <- cbind(test_four_reg, cur_test_four_reg)
      # }
      
      info(logger, "\n")
      
      require("forecast")
      # cur.model.name <- "seasonal auto.ar"
      cur.model.name <- "BoxCox auto.ar"
      fit.start.time <- Sys.time()
      cur.ar.fit <-
        auto.arima(
          cur_train_y_xts,
          max.p = cur_max_lag,
          max.d = 0,
          max.q = 0,
          max.order = cur_max_lag,
          seasonal = FALSE,
          # seasonal = TRUE,
          trace = verbose,
          stepwise = FALSE,
          # parallization of non-default ARIMA(p,d,q) is not supported
          lambda = lambda
        ) # dynamic ar model ONLY
      # cur.arima.fit <-
      #   auto.arima(
      #     cur_train_y_ts,
      #     max.p = cur_max_lag,
      #     seasonal = FALSE,
      #     trace = verbose,
      #     stepwise = FALSE
      #   )
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      cur.model.df <- length(cur.ar.fit$coef)
      if (cur.model.df >= cur_train_size) {
        warn(
          logger,
          sprintf(
            "%s '%s' unable to fit %d point with %d parameters",
            cur_out_comm_prefix,
            cur.model.name,
            cur_train_size,
            cur.model.df
          )
        )
      }
      cur.ar.pred <-
        forecast(cur.ar.fit, h = hold_out_h, lambda = lambda)
      
      require("Metrics")
      cur.pred.rmse <-
        rmse(as.numeric(cur_test_y_ts),
             as.numeric(cur.ar.pred$mean))
      cur.pred.mape <-
        mape(as.numeric(cur_test_y_ts),
             as.numeric(cur.ar.pred$mean))
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.ar.pred$mean))
      plot_color_list[[cur.model.name]] <-
        "blue"
      
      cur.model.name <- "BoxCox default auto.arima"
      # cur_max_p <- cur_max_lag # slow as HELL
      # max_q <- cur_max_lag # take FOREVER
      # default values
      max_p <- 5
      max_q <- 5
      max_P <- 2
      max_Q = 2
      cur_max_order <- max_p + max_q + max_P + max_Q
      fit.start.time <- Sys.time()
      cur.arima.fit <-
        auto.arima(
          cur_train_y_ts,
          max.p = max_p,
          max.order = cur_max_order,
          seasonal = FALSE,
          trace = verbose,
          stepwise = FALSE,
          lambda = lambda
        )
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      cur.model.df <- length(cur.arima.fit$coef)
      if (cur.model.df >= cur_train_size) {
        warn(
          logger,
          sprintf(
            "%s '%s' unable to fit %d point with %d parameters",
            cur_out_comm_prefix,
            cur.model.name,
            cur_train_size,
            cur.model.df
          )
        )
      }
      cur.arima.pred <-
        forecast(cur.arima.fit, h = hold_out_h, lambda = lambda)
      require("Metrics")
      cur.pred.rmse <-
        rmse(as.numeric(cur_test_y_ts),
             as.numeric(cur.arima.pred$mean))
      cur.pred.mape <-
        mape(as.numeric(cur_test_y_ts),
             as.numeric(cur.arima.pred$mean))
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arima.pred$mean))
      plot_color_list[[cur.model.name]] <-
        "cyan"
      
      cur.model.name <- "BoxCox seasonal default auto.arima"
      # cur_max_p <- cur_max_lag # slow as HELL
      # max_q <- cur_max_lag # take FOREVER
      # default values
      max_p <- 5
      max_q <- 5
      max_P <- 2
      max_Q = 2
      cur_max_order <- max_p + max_q + max_P + max_Q
      fit.start.time <- Sys.time()
      cur.arima.fit <-
        auto.arima(
          cur_train_y_ts,
          # max.p = max_p,
          # max.order = cur_max_order,
          seasonal = TRUE,
          # trace = verbose,
          stepwise = FALSE,
          lambda = lambda
        )
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      cur.model.df <- length(cur.arima.fit$coef)
      if (cur.model.df >= cur_train_size) {
        warn(
          logger,
          sprintf(
            "%s '%s' unable to fit %d point with %d parameters",
            cur_out_comm_prefix,
            cur.model.name,
            cur_train_size,
            cur.model.df
          )
        )
      }
      cur.arima.pred <-
        forecast(cur.arima.fit, h = hold_out_h, lambda = lambda)
      require("Metrics")
      cur.pred.rmse <-
        rmse(as.numeric(cur_test_y_ts),
             as.numeric(cur.arima.pred$mean))
      cur.pred.mape <-
        mape(as.numeric(cur_test_y_ts),
             as.numeric(cur.arima.pred$mean))
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arima.pred$mean))
      plot_color_list[[cur.model.name]] <-
        "cyan"
      
      cur.model.name <-
        "baggedETS" # significantly slower than tslm, auto.ar, TBATS
      cur.model.na <- FALSE
      fit.start.time <- Sys.time()
      # 9: stop("series is not periodic or has less than two periods")
      # 8: stl(ts(x.bc, frequency = freq), "per")
      # 7: bld.mbb.bootstrap(y, 100)
      # 6: lapply(bootstrapped_series, function(x) {
      #   mod <- ets(x, ...)
      # })
      tryCatch({
        # Error in stl(na.interp(xx), s.window = 7) :
        # series is not periodic or has less than two periods
        cur.model.fit <- baggedETS(cur_train_y_ts)
      }, error = function(e) {
        error(logger, sprintf("%s", as.character(e)))
        print(sys.calls())
        cur.model.na <<- TRUE
      })
      fit.end.time <- Sys.time()
      if (!cur.model.na)
        fit.time.taken <- fit.end.time - fit.start.time
      else
        fit.time.taken <- NA
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      if (!cur.model.na) {
        cur.model.pred <-
          forecast(cur.model.fit, h = hold_out_h)
        
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred$mean))
        cur.pred.rmse <-
          rmse(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        cur.pred.mape <-
          mape(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        plot_color_list[[cur.model.name]] <-
          "chocolate"
      } else{
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], rep(NA, hold_out_h))
      }
      
      
      cur.model.name <- "TBATS"
      cur_mul_sea_train_ts <-
        msts(cur_train_y_ts, seasonal.periods = c(24, 7 * 24))
      fit.start.time <- Sys.time()
      cur.model.fit <- tbats(cur_mul_sea_train_ts)
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      cur.model.df <- model.df(cur.model.fit)
      if (cur.model.df + 1 >= cur_train_size) {
        warn(
          logger,
          sprintf(
            "%s '%s' unable to fit %d point with %d parameters",
            cur_out_comm_prefix,
            cur.model.name,
            cur_train_size,
            cur.model.df
          )
        )
      }
      cur.model.pred <-
        forecast(cur.model.fit, h = hold_out_h)
      
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred$mean))
      cur.pred.rmse <-
        rmse(as.numeric(cur_test_y_ts),
             as.numeric(cur.model.pred$mean))
      cur.pred.mape <-
        mape(as.numeric(cur_test_y_ts),
             as.numeric(cur.model.pred$mean))
      plot_color_list[[cur.model.name]] <-
        "magentta"
      
      
      cur.model.name <- "tslm"
      if (ncol(train_four_reg) + 1 >= cur_train_size) {
        warn(
          logger,
          sprintf(
            "%s '%s' unable to fit %d point with %d parameters",
            cur_out_comm_prefix,
            cur.model.name,
            cur_train_size,
            ncol(train_four_reg) + 1
          )
        )
        require("usdm")
        v2 <- vifstep(train_four_reg, th = 10)
        sel_colnames <- as.character(v2@results$Variables)
        excluded_colnames <- as.character(v2@excluded)
        cur_filtered_xreg <- train_four_reg[, sel_colnames]
        if (length(excluded_colnames) > 0) {
          info(logger,
               sprintf(
                 "\tpredictors '%s' is excluded",
                 paste(excluded_colnames, collapse = ",")
               ))
        }
        train_four_reg <- cur_filtered_xreg
        test_four_reg <- test_four_reg[, sel_colnames]
      }
      fit.start.time <- Sys.time()
      cur.model.fit <- tslm(cur_train_y_ts ~ train_four_reg)
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      cur.model.df <- model.df(cur.model.fit)
      # cur.model.cv <- CV(cur.model.fit)
      cur.lm.coef.p <-
        summary(cur.model.fit)$coefficients[, 4] # https://stackoverflow.com/questions/5587676/pull-out-p-values-and-r-squared-from-a-linear-regression#5588638
      insig_pred_var_names <-
        names(cur.lm.coef.p[which(cur.lm.coef.p >= 0.05)])
      if (length(insig_pred_var_names)) {
        warn(
          logger,
          sprintf(
            "%s train sample size %d,  '%s' predictors %d/%d(%.2f%%) '%s' is statistically insignificant",
            cur_out_comm_prefix,
            cur_train_size,
            cur.model.name,
            length(insig_pred_var_names),
            length(coef(cur.model.fit)),
            length(insig_pred_var_names) * 1.0 / length(coef(cur.model.fit)) * 100,
            paste(insig_pred_var_names, collapse = ",")
          )
        )
      }
      # Error in forecast.lm(tslm.fit, h = hold_out_h) :
      # Variables not found in newdata
      cur.model.pred <-
        predict(cur.model.fit, newx = test_four_reg)
      
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred[1:hold_out_h]))
      cur.pred.rmse <-
        rmse(as.numeric(cur_test_y_ts), as.numeric(cur.model.pred))
      cur.pred.mape <-
        mape(as.numeric(cur_test_y_ts), as.numeric(cur.model.pred))
      plot_color_list[[cur.model.name]] <-
        "yellow"
      
      
      cur.model.name <- 'NNAR'
      cur.model.na <- FALSE
      fit.start.time <- Sys.time()
      tryCatch({
        # Error in stl(na.interp(xx), s.window = 7) :
        # series is not periodic or has less than two periods
        cur.model.fit <- nnetar(cur_train_y_ts)
      }, error = function(e) {
        error(logger, sprintf("%s", as.character(e)))
        print(sys.calls())
        cur.model.na <<- TRUE
      })
      fit.end.time <- Sys.time()
      if (!cur.model.na)
        fit.time.taken <- fit.end.time - fit.start.time
      else
        fit.time.taken <- NA
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      if (!cur.model.na) {
        cur.model.pred <- forecast(cur.model.fit, h = hold_out_h)
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred$mean))
        cur.pred.rmse <-
          rmse(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        cur.pred.mape <-
          mape(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        plot_color_list[[cur.model.name]] <-
          "black"
      } else {
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(rep(NA, hold_out_h)))
      }
      
      cur.model.name <- 'BoxCox_NNAR'
      cur.model.na <- FALSE
      fit.start.time <- Sys.time()
      tryCatch({
        # Error in stl(na.interp(xx), s.window = 7) :
        # series is not periodic or has less than two periods
        cur.model.fit <- nnetar(cur_train_y_ts, lambda = lambda)
      }, error = function(e) {
        error(logger, sprintf("%s", as.character(e)))
        print(sys.calls())
        cur.model.na <<- TRUE
      })
      fit.end.time <- Sys.time()
      if (!cur.model.na)
        fit.time.taken <- fit.end.time - fit.start.time
      else
        fit.time.taken <- NA
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          format(fit.time.taken)
        )
      )
      if (!cur.model.na) {
        cur.model.pred <-
          forecast(cur.model.fit, h = hold_out_h, lambda = lambda)
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.model.pred$mean))
        cur.pred.rmse <-
          rmse(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        cur.pred.mape <-
          mape(as.numeric(cur_test_y_ts),
               as.numeric(cur.model.pred$mean))
        plot_color_list[[cur.model.name]] <-
          "orange"
      } else {
        forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
          c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(rep(NA, hold_out_h)))
      }
      
      if (has_weekly_data) {
        fxreg_gam_mat <-
          data.frame(
            nDOY = as.integer(strftime(cur_test_datetime, "%j")),
            nWeek = as.integer(strftime(cur_test_datetime, "%W")),
            dayofweek = as.numeric(cur_test_datetime$wday),
            hourofday = as.numeric(format(cur_test_datetime, "%H")),
            tt = cur_hourly_vv_df$tt[cur_test_rng]
          )
      } else{
        fxreg_gam_mat <-
          data.frame(
            nDOY = as.integer(strftime(cur_test_datetime, "%j")),
            hourofday = as.numeric(format(index(
              cur_test_y_xts
            ), "%H")),
            tt = cur_hourly_vv_df$tt[cur_test_rng]
          )
      }
      # # suck at CV test
      # require("mgcv")
      # cur.model.name <- "GAM"
      # cur_decomp <- NULL
      # tryCatch({
      #   cur_decomp <-
      #     stl(cur_train_y_ts,
      #         s.window = "periodic",
      #         robust = TRUE)
      # }, error = function(e) {
      #   error(logger, as.character(e))
      #   print(sys.calls())
      # })
      # if (!is.null(cur_decomp)) {
      #   gam_mat <-
      #     data.frame(
      #       y = as.numeric(cur_train_y_ts),
      #       seasadj_y = as.numeric(seasadj(cur_decomp)),
      #       nWeek = as.integer(strftime(cur_train_datetime, "%W")),
      #       nDOY = as.integer(strftime(cur_train_datetime, "%j")),
      #       dayofweek = as.integer(cur_train_datetime$wday),
      #       hourofday = as.integer(format(cur_train_datetime, "%H")),
      #       tt = cur_hourly_vv_df$tt[cur_train_rng]
      #     )
      # } else{
      #   gam_mat <-
      #     data.frame(
      #       y = as.numeric(cur_train_y_ts),
      #       nWeek = as.integer(strftime(cur_train_datetime, "%W")),
      #       nDOY = as.integer(strftime(cur_train_datetime, "%j")),
      #       dayofweek = as.integer(cur_train_datetime$wday),
      #       hourofday = as.integer(format(cur_train_datetime, "%H")),
      #       tt = cur_hourly_vv_df$tt[cur_train_rng]
      #     )
      # }
      #
      # m <- length(unique(gam_mat))
      # cur_dayofweek_k <-
      #   min(length(unique(gam_mat[["dayofweek"]])) - 1, m)
      # cur_hourofday_k <-
      #   min(length(unique(gam_mat[["hourofday"]])) - 1, m)
      # cur_nWeek_k <- min(length(unique(gam_mat[["nWeek"]])) - 1, m)
      # cur_nDOY_k <- min(length(unique(gam_mat[["nDOY"]])) - 1, m)
      # ctrl <-
      #   list(niterEM = 0,
      #        msVerbose = TRUE,
      #        optimMethod = "L-BFGS-B")
      # fit.start.time <- Sys.time()
      # if (has_weekly_data) {
      #   cur_gam_fit <-
      #     gam(
      #       y ~ s(tt) +
      #         te(
      #           # https://stats.stackexchange.com/questions/234809/r-mgcv-why-do-te-and-ti-tensor-products-produce-different-surfaces#234817
      #           dayofweek,
      #           hourofday,
      #           k = c(cur_dayofweek_k, cur_hourofday_k),
      #           bs = c("cc", "cc")
      #         ),
      #       data = gam_mat,
      #       ctrl = ctrl
      #       # correlation = corARMA(
      #       #   form = ~ 1,
      #       #   p = y_freq
      #       # )
      #       # correlation = corAR1(form = ~ 1)
      #     )
      #
      #   # gamm(
      #   #   # y ~ s(dayofweek, k = 7) + s(hourofday, k = 24), # A term has fewer unique covariate combinations than specified maximum degrees of freedom
      #   #   y ~ s(dayofweek, k = cur_dayofweek_k, bs = "cc")
      #   #   + s(hourofday, k = cur_hourofday_k, bs = "cc"),
      #   #   data = gam_mat,
      #   #   correlation = corAR1(form = ~ 1)
      #   # )
      #
      # } else{
      #   cur_gam_fit <-
      #     gam(
      #       # y ~ s(dayofweek, k = 7) + s(hourofday, k = 24), # A term has fewer unique covariate combinations than specified maximum degrees of freedom
      #       y ~  s(tt) + s(hourofday, k = cur_hourofday_k, bs = "cc"),
      #       data = gam_mat,
      #       ctrl = ctrl
      #       # correlation = corAR1(form = ~ 1)
      #     )
      # }
      # fit.end.time <- Sys.time()
      # fit.time.taken <- fit.end.time - fit.start.time
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      # info(
      #   logger,
      #   sprintf(
      #     "%d points, %s '%s' model R-sq.(adj) = %.4f, fitting time taken: %s",
      #     cur_train_size,
      #     cur_out_comm_prefix,
      #     cur.model.name,
      #     summary(cur_gam_fit)$r.sq,
      #     format(fit.time.taken)
      #   )
      # )
      # layout(matrix(1:2, nrow = 1))
      # plot(cur_gam_fit, shade = TRUE)
      # par(mfrow = c(1, 1))
      # # vis.gam(cur_gam_fit, n.grid = 50, theta = 35, phi = 32, zlab = "",  ticktype = "detailed", color = "topo", main = "te(D, W)")
      # # vis.gam(cur_gam_fit,  main = "te(D, W)", plot.type = "contour", color = "terrain", contour.col = "black", lwd = 2)
      
      #
      # cur_gam_predict <-
      #   predict(cur_gam_fit, type = "response", newdata = fxreg_gam_mat)
      # cur.pred.mape <-
      #   mape(as.numeric(cur_test_y_ts), cur_gam_predict)
      # cur.pred.rmse <-
      #   rmse(as.numeric(cur_test_y_ts), cur_gam_predict)
      # forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #   c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(as.numeric(cur_gam_predict)))
      
      cur.model.name <- "BoxCox_GAM"
      cur_bc_decomp <- NULL
      tryCatch({
        cur_bc_decomp <-
          stl(cur_train_y_bc_ts,
              s.window = "periodic",
              robust = TRUE)
      }, error = function (e) {
        error(logger, as.character(e))
        print(sys.calls())
      })
      
      if (!is.null(cur_bc_decomp)) {
        gam_bc_mat <-
          data.frame(
            y = as.numeric(cur_train_y_bc_ts),
            seasadj_y = as.numeric(seasadj(cur_bc_decomp)),
            nWeek = as.integer(strftime(cur_train_datetime, "%W")),
            nDOY = as.integer(strftime(cur_train_datetime, "%j")),
            dayofweek = as.integer(cur_train_datetime$wday),
            hourofday = as.integer(format(cur_train_datetime, "%H")),
            tt = cur_hourly_vv_df$tt[cur_test_rng]
          )
        
      } else {
        gam_bc_mat <-
          data.frame(
            y = as.numeric(cur_train_y_bc_ts),
            nWeek = as.integer(strftime(cur_train_datetime, "%W")),
            nDOY = as.integer(strftime(cur_train_datetime, "%j")),
            dayofweek = as.integer(cur_train_datetime$wday),
            hourofday = as.integer(format(cur_train_datetime, "%H")),
            tt = cur_hourly_vv_df$tt[cur_test_rng]
          )
      }
      m <- length(unique(gam_bc_mat))
      cur_dayofweek_k <-
        min(length(unique(gam_bc_mat[["dayofweek"]])) - 1, m)
      cur_hourofday_k <-
        min(length(unique(gam_bc_mat[["hourofday"]])) - 1, m)
      cur_nWeek_k <-
        min(length(unique(gam_bc_mat[["nWeek"]])) - 1, m)
      cur_nDOY_k <- min(length(unique(gam_bc_mat[["nDOY"]])) - 1, m)
      ctrl <-
        list(niterEM = 0,
             msVerbose = TRUE,
             optimMethod = "L-BFGS-B")
      fit.start.time <- Sys.time()
      if (has_weekly_data) {
        cur.gam.bc.fit <-
          gam(
            y ~ s(tt) + te(
              hourofday,
              dayofweek,
              k = c(cur_hourofday_k, cur_dayofweek_k),
              bs = c("cc", "cc")
            ),
            data = gam_bc_mat,
            ctrl = ctrl
          )
      } else {
        cur.gam.bc.fit <-
          gam(
            y ~ s(tt) + s(hourofday,
                          k = cur_hourofday_k,
                          bs = "cc"),
            data = gam_bc_mat,
            ctrl = ctrl
          )
      }
      fit.end.time <- Sys.time()
      fit.time.taken <- fit.end.time - fit.start.time
      model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
      info(
        logger,
        sprintf(
          "%d points, %s '%s' model R-sq.(adj) = %.4f, fitting time taken: %s",
          cur_train_size,
          cur_out_comm_prefix,
          cur.model.name,
          summary(cur.gam.bc.fit)$r.sq,
          format(fit.time.taken)
        )
      )
      cur.gam.bc.predict <-
        InvBoxCox(as.numeric(
          predict(
            cur.gam.bc.fit,
            type = "response",
            h = hold_out_h,
            newdata = fxreg_gam_mat
          )
        ),
        lambda = lambda)
      cur.pred.mape <-
        mape(cur_test_y_vec, cur.gam.bc.predict)
      cur.pred.rmse <-
        rmse(cur_test_y_vec, cur.gam.bc.predict)
      forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
        c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(as.numeric(cur.gam.bc.predict)))
      
      # cur.model.name <- "ARIMAX using fourier terms"
      # cur_fit_na <- FALSE
      # cur.model.na <- FALSE
      # require("parallel")
      # avail.cores <- floor(detectCores() / 2)
      # fit.start.time <- Sys.time()
      # tryCatch({
      #   cur.model.fit <-
      #     auto.arima(
      #       cur_train_y_ts,
      #       xreg = train_four_reg,
      #       stepwise = FALSE,
      #       parallel = TRUE,
      #       num.cores = avail.cores # default value of this parameter is NOT effective
      #     )
      # }, error = function(e) {
      #   error(logger, sprintf("%s", as.character(e)))
      #   print(sys.calls())
      #   cur.model.na <<- TRUE
      # })
      # fit.end.time <- Sys.time()
      # fit.time.taken <-
      #   fit.end.time - fit.start.time  # INCREDIBLY slower than other model with fourier predictors like tslm etc even with multiple CPU cores utilization.
      # model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      # c(model_fit_time_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(fit.time.taken))
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
      # if (cur.model.df >= cur_train_size) {
      #   print(
      #     sprintf(
      #       "%s '%s' unable to fit %d point with %d parameters",
      #       cur_out_comm_prefix,
      #       cur.model.name,
      #       cur_train_size,
      #       cur.model.df
      #     )
      #   )
      # }
      
      # if (!cur.model.na) {
      #   cur.arimax.pred <-
      #     forecast(cur.model.fit, xreg = test_four_reg, h = hold_out_h)
      #
      #   require("Metrics")
      #   cur.pred.rmse <- rmse(cur_test_y_ts, cur.arimax.pred$mean)
      #   forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #     c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(cur.arimax.pred$mean))
      # } else {
      #   forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]] <-
      #     c(forecast_list[[cur_bid_str]][[cur_pid_str]][[cur_train_h_str]][[cur.model.name]], list(rep(NA, hold_out_h)))
      # }
      
      # short_lm_model <- tslm(train_y_ts ~ train_four_reg)
      # CV(short_lm_model)
      # cur_lm_fit_name <-
      #   paste(
      #     coefficients(short_lm_model),
      #     names(coefficients(short_lm_model)),
      #     sep = "*",
      #     collapse = "+"
      #   )
      # cur_lm_predict <-
      #   predict(short_lm_model, newx = test_four_reg, h = hold_out_h)
      # # cv <- mean((residuals(obj) / (1 - hatvalues(obj))) ^ 2, na.rm = TRUE)
      # require("glmnet")
      # cv_lasso_fit <- cv.glmnet(train_four_reg, train_y)
      # # best_lambda <- lasso_fit$lambda[which.min(lasso_fit$lambda)]
      # best_lambda <-
      #   cv_lasso_fit$lambda.min # or cv_lasso_fit$lambda.1se
      # lasso_fit <-
      #   glmnet(train_four_reg, train_y, lambda = best_lambda)
      # plot(lasso_fit)
      # lasso_fcast <-
      #   predict(
      #     lasso_fit,
      #     newx = test_four_reg,
      #     lambda = best_lambda,
      #     type = "coefficients",
      #     h = hold_out_h
      #   )
      # plot(lasso_fcast, type = "b,c")
      # lines(test_y, col = "red")
      # require("Metrics")
      # rmse(test_y, lasso_fcast)
      # }
      
      # cat("\n")
      info(logger, "\n")
    } # for every CV train size
    
    subplot_cnt <- length(names(forecast_list))
    # graphics.off()
    # par("mar")
    # par(mar=c(1,1,1,1))
    # png("test_par.png",
    #     width = 1920,
    #     height = 720 * sub_plot_num)
    ggplot_list <- c()
    pred_plot_mat <- NULL
    # op <- par(mfrow = c(subplot_cnt, 1))
    for (min_train_h in names(forecast_list[[cur_bid_str]][[cur_pid_str]])) {
      cur_plot_title <- ""
      cur_plot_main_title <- ""
      cur_plot_sub_title <- ""
      cur_plot_mat <- NULL
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
        cur_plot_mat <-
          rbind(cur_plot_mat,
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
          sprintf("train size = %s",
                  min_train_h)
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
      cur_plot_mat <-
        rbind(cur_plot_mat,
              data.frame(
                x = index(cur_actual_y_xts),
                y = as.numeric(cur_actual_y_ts),
                model.name = rep("Actual VV", length(cur_actual_y_ts))
              ))
      pred_plot_mat <- rbind(pred_plot_mat, cur_plot_mat)
      require("scales")
      cur_gg_p <-
        ggplot(cur_plot_mat, aes(x = x, y = y, color = model.name)) + geom_line() + geom_point() +
        scale_x_datetime(labels = date_format("%Y-%m-%d %H:%M"),
                         minor_breaks = date_breaks("2 hour")) +
        theme(axis.text.x = element_text(angle = 60,
                                         # vjust = 0.5,
                                         hjust = 1)) +
        xlab("Datetime") + ylab("Hourly Video Visit") + labs(title = cur_plot_main_title, subtitle = cur_plot_sub_title)
      ggplot_list <- c(ggplot_list, list(cur_gg_p))
      # plot_cols <- 1:ncol(cur_plot_mat)
      # matplot(
      #   cur_plot_mat,
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
    require("animation")
    saveGIF({
      for (p in ggplot_list)
        print(p)
    }, movie.name = sprintf("%s/%s_pred.gif", gif_file_dir, out_file_desc_prefix), interval = 2, ani.width = 1920, ani.height = 1080)
    
    # par(op)
    # dev.off()
    ggplot() + geom_line(data = pred_plot_mat, aes(x = x, y = y, color = model.name)) +  geom_point() +
      scale_x_datetime(
        labels = date_format("%Y-%m-%d %H:%M"),
        breaks = date_breaks("1 day"),
        minor_breaks = date_breaks("12 hour")
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
    
    # plot daily VV prediction by aggregation
    daily_pred_plot_mat <-
      aggregate(
        pred_plot_mat$y,
        by = list(
          x = as.Date(as.character(pred_plot_mat$x)),
          # as.Date("2017-02-07 00:00:00 CST") == "2017-02-06"!!!!
          model.name = pred_plot_mat$model.name
        ),
        FUN = sum,
        na.rm = TRUE
      )
    grp_colnames <- colnames(daily_pred_plot_mat)
    colnames(daily_pred_plot_mat)[length(grp_colnames)] <- "y"
    ggplot() + geom_line(data = daily_pred_plot_mat, aes(x = x, y = y, color = model.name)) +  geom_point() +
      scale_x_date(labels = date_format("%Y-%m-%d"),
                   breaks = date_breaks("1 day")) +  theme(axis.text.x = element_text(angle = 60,
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
    
    # plot train size versus error metrics
    mape_column_list <- c()
    for (min_train_h in names(mape_list[[cur_bid_str]][[cur_pid_str]])) {
      for (model.name in names(mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]])) {
        # cat differenct CV size stat value
        mape_column_list[[model.name]] <-
          c(mape_column_list[[model.name]], mape_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]])
      }
    }
    plot_x <-
      as.integer(names(mape_list[[cur_bid_str]][[cur_pid_str]]))
    mape_plot_df <- data.frame()
    cur_df <- NULL
    for (model.name in names(mape_column_list)) {
      cur_df <-
        data.frame(
          x = plot_x,
          y = as.numeric(mape_column_list[[model.name]]),
          model.name = rep(model.name, length(plot_x))
        )
      mape_plot_df <- rbind(mape_plot_df, cur_df)
    }
    
    rmse_column_list <- c()
    for (min_train_h in names(rmse_list[[cur_bid_str]][[cur_pid_str]])) {
      for (model.name in names(rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]])) {
        rmse_column_list[[model.name]] <-
          c(rmse_column_list[[model.name]], rmse_list[[cur_bid_str]][[cur_pid_str]][[min_train_h]][[model.name]])
      }
    }
    plot_x <-
      as.integer(names(rmse_list[[cur_bid_str]][[cur_pid_str]]))
    rmse_plot_df <- data.frame()
    cur_df <- NULL
    for (model.name in names(rmse_column_list)) {
      cur_df <-
        data.frame(
          x = plot_x,
          y = as.numeric(rmse_column_list[[model.name]]),
          model.name = rep(model.name, length(plot_x))
        )
      rmse_plot_df <- rbind(rmse_plot_df, cur_df)
    }
    require("grid")
    require("gridExtra")
    ggplot_err_p_list <- c()
    mape_p <-
      ggplot() + geom_line(data = subset(mape_plot_df, !is.na(y)),
                           aes(x = x, y = y, color = model.name)) + geom_point() + xlab("Train Sample Size") + ylab("MAPE Error")
    # lines(test_y_ts, col = "red")
    ggplot_err_p_list <- c(ggplot_err_p_list, list(mape_p))
    
    rmse_p <-
      ggplot() + geom_line(data = subset(rmse_plot_df, !is.na(y)),
                           aes(x = x, y = y, color = model.name)) + geom_point() + xlab("Train Sample Size") + ylab("RMSE Error")
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
      sapply(mape_column_list, function(x) {
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
    
    cur_CV_mean_rmse <-
      sapply(rmse_column_list, function(x) {
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
    cur_cv_info <-
      list(
        pid = cur_pid_str,
        pid.name = cur_pid_name,
        data.set = cur_hourly_vv_df,
        CV.seq = cur_train_h_seq,
        RMSE = list(
          name = names(cur_CV_best_rmse_idx),
          value = cur_CV_mean_rmse[cur_CV_best_rmse_idx]
        ),
        MAPE = list(
          name = names(cur_CV_best_mape_idx),
          value = cur_CV_mean_mape[cur_CV_best_mape_idx]
        ),
        daily.pred.plot.mat = daily_pred_plot_mat,
        pred.plot.mat = pred_plot_mat,
        rmse.vec.list = lapply(rmse_column_list, unlist),
        mape.vec.list = lapply(mape_column_list, unlist),
        start.date = cur_start_date_str,
        end.date = cur_pred_end_date_str
      )
    best_model_list[[cur_bid_str]][[cur_pid_str]] = cur_cv_info
    
    # cat("\n")
    info(logger, "\n")
  } # for every pid
  mysql_dbDisconnectAll(mysql.drv)
} # for every bid


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
    ggplot(data = subset(cur_model_comp_m, !is.na(value)),
           aes(x = model.name, y = value, fill = model.name)) +
      geom_boxplot() +  guides(fill = guide_legend(title = "模型类别")) +
      labs(
        title = sprintf(
          "%s '%s' %s-%s CV Error Metric",
          cur_bid_name,
          cur_pid_name,
          start_date_str_list[[i]],
          pred_end_date_str_list[[i]]
        )
      ) +
      ylab("模型名称") + xlab("24小时预测误差MAPE")
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
      melt(model_fit_time_list[[cur_bid_str]][[cur_pid_str]])
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
        df$difftime <- as.numeric(df$difftime, units = "secs")
        df
      }))
    ggplot(
      data = subset(cur_model_fit_time_plot_df, !is.na(difftime)),
      aes(x = model.name, y = difftime, fill = model.name)
    ) + geom_boxplot() +
      theme(axis.text.x = element_text(angle = 60,
                                       # vjust = 0.5,
                                       hjust = 1)) + xlab("模型名称") + ylab("运行时间(秒)") +
      labs(title = sprintf("%s端 '%s' 模型拟合时间比较", cur_bid_name, cur_pid_name))
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
    data = subset(model_fit_time_mean_df,!is.na(mean_secs)),
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
      "%s/%s_top%d_model_fit_time_boxplot_%s.png",
      png_file_dir,
      cur_bid_name,
      top_pid_cnt,
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
    labs(title = sprintf("%s端模型MAPE指标比较", cur_bid_name))
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
      "%s/%s_top%d_model_comp_heatmap_%s.png",
      png_file_dir,
      cur_bid_name,
      top_pid_cnt,
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
    ylab("模型名称") + xlab("24小时预测误差MAPE")
  # ggplotly() # interactive boxplot for RStudio
  ggsave(
    sprintf(
      "%s/%s_top%d_model_comp_boxplot_%s.png",
      png_file_dir,
      cur_bid_name,
      top_pid_cnt,
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
  write(
    sprintf(
      "pid\tpid_name\tstart_date\tpred_end_date\tbest_model_name\tvalue"
    ),
    cur_rmse_stat_file,
    append = TRUE,
    sep = "\n"
  )
  cur_mape_stat_file <-
    sprintf(
      "%s/%s_top%d_CV_MAPE_stat_%s",
      stat_file_dir,
      cur_bid_name,
      top_pid_cnt,
      cur_time_str
    )
  mape_file_con <- file(cur_mape_stat_file, 'w')
  write(
    sprintf(
      "pid\tpid_name\tstart_date\tpred_end_date\tbest_model_name\tvalue"
    ),
    cur_mape_stat_file,
    append = TRUE,
    sep = "\n"
  )
  for (cur_pid_idx in seq_along(names(best_model_list[[cur_bid_str]]))) {
    cur_pid_str <- names(best_model_list[[cur_bid_str]])[[cur_pid_idx]]
    cur_cv_info <- best_model_list[[cur_bid_str]][[cur_pid_str]]
    cur_CV_cnt <- length(cur_cv_info$CV.seq)
    stopifnot(!is.null(cur_cv_info))
    cur_rmse_info <-
      cur_cv_info$RMSE
    write(
      sprintf(
        "%s\t%s\t%s\t%s\t%s\t%.4f",
        cur_cv_info$pid,
        cur_cv_info$pid.name,
        cur_cv_info$start.date,
        cur_cv_info$end.date,
        cur_rmse_info$name,
        cur_rmse_info$value
      ),
      cur_rmse_stat_file,
      append = TRUE,
      sep = "n"
    )
    cur_mape_info <-
      cur_cv_info$MAPE
    write(
      sprintf(
        "%s\t%s\t%s\t%s\t%s\t%.4f",
        cur_cv_info$pid,
        cur_cv_info$pid.name,
        cur_cv_info$start.date,
        cur_cv_info$end.date,
        cur_mape_info$name,
        cur_mape_info$value
      ),
      cur_mape_stat_file,
      append = TRUE,
      sep = "n"
    )
  }
  for (pid_idx in seq_along(skipped_pid_list[[cur_bid_str]])) {
    pid <- skipped_pid_list[[cur_bid_str]][[pid_idx]]
    pid_name <- skipped_pid_name_list[[cur_bid_str]][[pid_idx]]
    write(
      sprintf(
        "%s\t'%s'\tNA\tNA\tNA\tNA",
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
        "%s\t'%s'\tNA\tNA\tNA\tNA",
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