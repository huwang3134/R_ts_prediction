# https://robjhyndman.com/hyndsight/dailydata/
# https://stats.stackexchange.com/questions/14948/seasonal-data-forecasting-issues#answer-14956
# https://robjhyndman.com/hyndsight/longseasonality/
require("RPostgreSQL")
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

max_p <- 7
max_q <- 7
max_P <- 7
max_q <- 7
max_order <- max_p + max_q + max_P + max_Q

predict_ts_len <- 7

cur_bid_str <- '(9)' # aPhone
cur_start_date_str <- "20170101"
cur_pred_end_date_str <- "20171231"
cur_pid <- "308710"
vv_sql <- sprintf(
  "select date,sum(vv) as vv from vv_pid_day where bid in %s and date between %s and %s and pid = %s group by date, pid order by date, vv desc;",
  cur_bid_str,
  cur_start_date_str,
  cur_pred_end_date_str,
  cur_pid
)
print(vv_sql)

# query the top pid from postgreSQL
sql.start.time <- Sys.time()
pid_vv_data <- dbGetQuery(con, vv_sql)
sql.end.time <- Sys.time()
sql.time.taken <- sql.end.time - sql.start.time
last_year_daily_data <- pid_vv_data
last_year_train_ts <- ts(last_year_daily_data$vv, frequency = 365.25)
last_year_daily_data$date <- as.character(last_year_daily_data$date)
last_year_x_lab <- as.Date(last_year_daily_data$date, format = "%Y%m%d")



yesterday <- Sys.Date() - 1
yesterday_str <- format(Sys.Date() - 1, "%Y%m%d")
cur_bid_str <- '(9)' # aPhone
cur_start_date_str <- "20180101"
cur_pred_end_date_str <- yesterday_str
cur_pid <- "320519"
vv_sql <- sprintf(
  "select date,sum(vv) as vv from vv_pid_day where bid in %s and date between %s and %s and pid = %s group by date, pid order by date, vv desc;",
  cur_bid_str,
  cur_start_date_str,
  cur_pred_end_date_str,
  cur_pid
)
print(vv_sql)

# query the top pid from postgreSQL
sql.start.time <- Sys.time()
pid_vv_data <- dbGetQuery(con, vv_sql)
sql.end.time <- Sys.time()
sql.time.taken <- sql.end.time - sql.start.time
this_year_daily_data <- pid_vv_data

cur_train_data <- rbind(last_year_daily_data, this_year_daily_data)
cur_train_ts <- ts(head(cur_train_data$vv,-predict_ts_len), frequency = 365.25)
cur_test_ts <- ts(tail(cur_train_data$vv, predict_ts_len), frequency = 365.25)
cur_train_data$date <- as.character(cur_train_data$date)
cur_train_data$Date <- as.Date(cur_train_data$date, format = "%Y%m%d")
cur_x_lab <- cur_train_data$Date

xreg <- fourier(cur_train_ts, K = 5)
fxreg <- fourier(cur_train_ts, K = 5, h = predict_ts_len)

fit.start.time <- Sys.time()
cur_fit <- auto.arima(
  cur_train_ts,
  max.p = max_p,
  max.q = max_q,
  max.d = max_d,
  max.D = max_D,
  xreg = xreg,
  max.order = max_order,
  approximation = FALSE,
  stepwise = FALSE,
  seasonal = TRUE,
  trace = TRUE
  # trace = FALSE,
  # # trace = TRUE
  # parallel = TRUE
)
fit.end.time <- Sys.time()
fit.time.taken <- fit.end.time - fit.start.time

cur_fcast <- forecast(cur_fit, xreg = fxreg, h = predict_ts_len)
cur_mape <- mape(as.numeric(cur_fcast$mean), as.numeric(cur_test_ts))


cur_actual_df <-
  subset(cur_train_data, Date >= as.Date("2018-01-01"))
plot.df <- data.frame(pred = cur_actual_df$vv, date = cur_actual_df$Date)
plot.df$label <- rep("Train actual daily VV", nrow(plot.df))

pred.plot.df <-
  data.frame(pred = as.numeric(cur_fcast$mean),
             date = tail(cur_x_lab, predict_ts_len))
pred.plot.df$label <-
  rep(sprintf("'%s' with fourier xreg forecast",
              as.character(cur_fit)),
      nrow(pred.plot.df))
plot.df <-
  rbind(plot.df, pred.plot.df)

# plot.df <-
#   data.frame(pred = as.numeric(head(cur_train_ts, predict_ts_len)),
#              date = head(cur_x_lab, predict_ts_len))
# plot.df$label <- rep("This year actual daily VV", nrow(plot.df))
# 
# last.year.plot.df <-
#   data.frame(pred = as.numeric(head(last_year_train_ts, predict_ts_len)),
#              date = head(cur_x_lab, predict_ts_len)) # using this year's date for view convience
# last.year.plot.df$label <-
#   rep("Last year actual daily VV", nrow(last.year.plot.df))
# plot.df <-
#   rbind(plot.df, last.year.plot.df)
# 
# pred.plot.df <-
#   data.frame(pred = as.numeric(cur_fcast$mean),
#              date = head(cur_x_lab, predict_ts_len))
# pred.plot.df$label <-
#   rep(sprintf("'%s' with fourier xreg\tforecast",
#               as.character(cur_fit)),
#       nrow(pred.plot.df))
# plot.df <-
#   rbind(plot.df, pred.plot.df)

require("ggplot2")
p <- ggplot() + scale_x_date("日期")
p <-
  p + geom_line(data = plot.df, aes(x = date, y = pred, colour = label)) + geom_point() + ylab("VV值")
plot(residuals(cur_fit))
dbDisconnect(con)