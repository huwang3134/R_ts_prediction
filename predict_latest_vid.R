options(error = traceback)
rm(list = ls())

program.start.time <- Sys.time()

mysql_dbDisconnectAll <- function(drv) {
  ile <- length(dbListConnections(drv))
  lapply(dbListConnections(drv), function(conn) {
    # dbClearResult(dbListResults(conn)[[1]])
    dbDisconnect(conn)
  })
  cat(sprintf("%s connection(s) closed.\n", ile))
}


postgresql_dbDisconnectAll <- function(drv) {
  ile <- length(dbListConnections(drv))
  lapply(dbListConnections(drv), function(conn) {
    # dbClearResult(dbListResults(conn)[[1]])
    dbDisconnect(conn)
  })
  cat(sprintf("%s connection(s) closed.\n", ile))
}


tryCatch({
  mysql_dbDisconnectAll(mysql.drv)  # timeout or have pending rows
}, error = function (e) {
  cat(as.character(e))
})

tryCatch({
  postgresql_dbDisconnectAll(hourly.vv.pg.drv)  # timeout or have pending rows
}, error = function (e) {
  cat(as.character(e))
})

tryCatch({
  postgresql_dbDisconnectAll(daily.vv.pg.drv)  # timeout or have pending rows
}, error = function (e) {
  cat(as.character(e))
})


require("log4r")
logger <- create.logger(logfile = "", level = "INFO")

require("RPostgreSQL")
require("RMySQL")

hourly.vv.earliest.date <-
  as.Date(strptime("2017-05-05", "%Y-%m-%d"))

top_pid_cnt <- 30
bid_str_list <- c("9", "12")  # aPhone, iPhone
bid_name_list <- c("aphone", "iphone")
names(bid_str_list) <- bid_name_list
names(bid_name_list) <- bid_str_list
bid_str_num <- length(bid_str_list)
total_task_num <- bid_str_num * top_pid_cnt

yesterday <- Sys.Date() - 1
yesterday_str <- format(yesterday, "%Y%m%d")
out_file_dir <- "out/vid"
# dir.create(out_file_dir, showWarnings = FALSE, recursive = TRUE)
vv_file_dir <- sprintf("%s/VV", out_file_dir)
dir.create(vv_file_dir, showWarnings = FALSE, recursive = TRUE)
err_file_dir <- sprintf("%s/err/%s", out_file_dir, yesterday_str)
dir.create(err_file_dir, showWarnings = FALSE, recursive = TRUE)
png_file_dir <- sprintf("%s/png/%s", out_file_dir, yesterday_str)
dir.create(png_file_dir, showWarnings = FALSE, recursive = TRUE)
# cur_bid_str <- "(12)"
# cur_pid_str <- "321779"

vv_stat_days_count <- 30
vv_start_date <- yesterday - vv_stat_days_count + 1
vv_start_date_str <- format(vv_start_date, "%Y%m%d")
vv_end_date_str <- yesterday_str

max_day_time_span <- 6
mru_vid_grp_cnt <- 4
clip_info_list <- c()
serialno_missing_pid_list <- c()
skipped_pid_list <- c()
hourly_vv_df_list <- c(c())
mape_list <- c()
rmse_list <- c()

cur_time <- Sys.time()
cur_time_str <- format(cur_time, "%Y%m%d-%H:%M")
err_out_file <- sprintf("%s/err_%s", err_file_dir, cur_time_str)
emp_sql_file <-
  sprintf("%s/empty_sql_%s", err_file_dir, cur_time_str)

for (j in seq_along(bid_str_list)) {
  cur_bid_str <- bid_str_list[[j]]
  cur_bid_name <- bid_name_list[[j]]
  
  pid_vv_sql <-
    sprintf(
      "select pid, sum(vv) as vv from vv_pid_day where bid in (%s) and date between %s and %s and pid != -1 group by pid order by vv desc limit %d; ",
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
  pg.daily.vv.conn <- dbConnect(
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
  pid_vv_data <- dbGetQuery(pg.daily.vv.conn, pid_vv_sql)
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
  # pid_str_list <- c("317663", "321779", "321502")
  
  mysql.drv <- MySQL()
  mysql.conn = dbConnect(
    mysql.drv,
    user = 'bigdata_r',
    password = 'B1gD20t0_r',
    port = 3306,
    host = '10.27.106.230',
    dbname = "mgmd"
  )
  dbSendQuery(mysql.conn, "SET NAMES utf8")
  
  cur_pid_sql_cond_str <-
    paste0('(', paste(pid_str_list, collapse = ","), ')')
  clip_info_sql <-
    sprintf(
      'select clipId, clipName, updateTime, releaseTime  from asset_v_clips where clipId in %s;',
      cur_pid_sql_cond_str
    )
  print(clip_info_sql)
  clip_info_list[[cur_bid_str]] <-
    dbGetQuery(mysql.conn, clip_info_sql)
  clip_info_list[[cur_bid_str]]$updateTime <-
    strptime(clip_info_list[[cur_bid_str]]$updateTime, format = "%Y-%m-%d %H:%M:%S")
  clip_info_list[[cur_bid_str]]$releaseTime <-
    strptime(clip_info_list[[cur_bid_str]]$releaseTime, format = "%Y-%m-%d %H:%M:%S")
  pid_name_list <- clip_info_list[[cur_bid_str]]$clipName
  pid_name_list <-
    gsub(" ", "", pid_name_list, fixed = TRUE)  # remove white spaces
  pid_str_list <- as.character(clip_info_list[[cur_bid_str]]$clipId)
  names(pid_str_list) <- pid_name_list
  names(pid_name_list) <- pid_str_list
  
  for (i in seq_along(pid_str_list)) {
    cur_pid_str <- pid_str_list[[i]]
    cur_pid_name <- names(pid_str_list)[[i]]
    names(cur_pid_str) <- cur_pid_name
    cur_out_comm_prefix <-
      sprintf("%s '%s'", cur_bid_name, cur_pid_name)
    
    video_type = 1 # full-length video, exclude trailer, extras, Behind The Scene etc.
    vid_info_sql <- paste0(
      c(
        "select a.partId as vid, a.partName as vid_name, a.mppOnlineTime, b.fstlvlType, a.serialno, a.clipId,",
        "b.clipName, b.playTime, b.newPartUpTime, a.updateTime, a.createTime, a.releaseTime from (",
        "select partId, partName, serialno, clipId, mppOnlineTime, updateTime, createTime, releaseTime from asset_v_parts",
        sprintf("where clipId in (%s) and", cur_pid_str),
        sprintf("isIntact = %d", video_type),
        ") a",
        "inner join asset_v_clips b on a.clipId = b.clipId order by mppOnlineTime DESC, serialno DESC;"
      ),
      collapse = " "
    )
    print(vid_info_sql)
    vid_time_info_df <- dbGetQuery(mysql.conn, vid_info_sql)
    vid_time_info_df$newPartUpTime <-
      strptime(vid_time_info_df$newPartUpTime, "%Y-%m-%d %H:%M:%S")
    vid_time_info_df$updateTime <-
      strptime(vid_time_info_df$updateTime, "%Y-%m-%d %H:%M:%S")
    vid_time_info_df$createTime <-
      strptime(vid_time_info_df$createTime, "%Y-%m-%d %H:%M:%S")
    vid_time_info_df$releaseTime <-
      strptime(vid_time_info_df$mppOnlineTime, "%Y-%m-%d %H:%M:%S")
    vid_time_info_df <-
      subset(vid_time_info_df,!is.na(releaseTime))
    if (nrow(vid_time_info_df) < 2) {
      warn(
        logger,
        sprintf(
          "%s has no video release time info yet, skipped",
          cur_out_comm_prefix
        )
      )
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(cur_pid_str))
      next
    }
    
    vid_time_rng_df <- NULL
    uniq_vid_rel_time <- unique(vid_time_info_df$releaseTime)
    for (row_i in 1:nrow(vid_time_info_df)) {
      cur_vid = vid_time_info_df[row_i, "vid"]
      cur_vid_name = vid_time_info_df[row_i, "vid_name"]
      cur_serial_no <- vid_time_info_df[row_i, "serialno"]
      cur_rel_time <- vid_time_info_df[row_i, "releaseTime"]
      cur_rel_time_level_idx <-
        which(uniq_vid_rel_time == cur_rel_time)
      maximum_allow_time <-
        strptime(sprintf("%s 00:00:00", as.character(yesterday)), format = "%Y-%m-%d %H:%M:%S")
      if (cur_rel_time > maximum_allow_time) {
        cur_rel_time <- maximum_allow_time
      }
      cur_rel_date <-
        as.Date(cur_rel_time)
      # if (cur_rel_date > yesterday){
      #   cur_rel_date <- yesterday
      # }
      if (1 == cur_rel_time_level_idx) {
        cur_real_end_date <- cur_rel_date + max_day_time_span
        cur_real_end_time <-
          strptime(sprintf("%s 23:59:59", as.character(cur_real_end_date)), format = "%Y-%m-%d %H:%M:%S")
      } else{
        cur_real_end_date <-
          as.Date(uniq_vid_rel_time[cur_rel_time_level_idx - 1])
        cur_real_end_time <-
          uniq_vid_rel_time[cur_rel_time_level_idx - 1]
      }
      if (cur_real_end_date > yesterday) {
        cur_real_end_date <- yesterday
        cur_real_end_time <-
          maximum_allow_time
      }
      vid_time_rng_df <-
        rbind(
          vid_time_rng_df,
          data.frame(
            vid = cur_vid,
            vid_name = cur_vid_name,
            serialno = cur_serial_no,
            start_date = cur_rel_date,
            start_time = cur_rel_time,
            end_date = cur_real_end_date,
            end_time = cur_real_end_time
          )
        )
    } # for every latest vid(s)
    vid_time_rng_df$vid_name <-
      as.character(vid_time_rng_df$vid_name)
    
    # merge similar release time vid
    for (vid_i in seq(2, nrow(vid_time_rng_df))) {
      # time decreasing order
      if (as.numeric(vid_time_rng_df[vid_i - 1, "start_time"] - vid_time_rng_df[vid_i, "start_time"], units = "secs") < 60) {
        vid_time_rng_df[vid_i - 1, "start_time"] <-
          vid_time_rng_df[vid_i, "start_time"]
      }
      if (as.numeric(vid_time_rng_df[vid_i - 1, "end_time"] - vid_time_rng_df[vid_i, "end_time"], units = "secs") < 60) {
        vid_time_rng_df[vid_i - 1, "end_time"] <-
          vid_time_rng_df[vid_i, "end_time"]
      }
    }
    # check hourly VV availablity
    cur_latest_rel_date <- as.Date(vid_time_rng_df[1, "end_time"])
    if (cur_latest_rel_date <= hourly.vv.earliest.date) {
      warn(
        logger,
        sprintf(
          "%s has NO valid Hourly VV data in DB, skipped.",
          cur_out_comm_prefix
        )
      )
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(cur_pid_str))
      next
    }
    
    vid_time_rng_grp_df <-
      aggregate(vid ~ start_date + start_time + end_date + end_time,
                data = vid_time_rng_df,
                FUN = c)  # this change the date order of vid_time_rng_df, and the factor names is lost when formaulat is c(factor1, factor2) ~ grp_var1 + grp_var2
    vid_time_rng_grp_df <-
      vid_time_rng_grp_df[with(vid_time_rng_grp_df,
                               order(end_date,
                                     start_date,
                                     decreasing = T)), ]
    # append vid name
    if ("list" == class(vid_time_rng_grp_df[, "vid"])) {
      cur_vid_grp_list <-
        as.list(vid_time_rng_grp_df[, "vid"]) # list OR matrix
      cur_vid_names_list <- c(c())
      for (elem_i in seq_along(cur_vid_grp_list)) {
        cur_vid_name_list <- c()
        for (col_j in seq_along(cur_vid_grp_list[[elem_i]])) {
          cur_vid <- cur_vid_grp_list[[elem_i]][[col_j]]
          # print(class(cur_vid))
          cur_vid_name <-
            vid_time_info_df[which(vid_time_info_df$vid == cur_vid), "vid_name"]
          # print(cur_vid_name)
          cur_vid_name_list <-
            c(cur_vid_name_list, cur_vid_name)
        }
        cur_vid_names_list <-
          c(cur_vid_names_list, list(cur_vid_name_list))
      }
      
      vid_time_rng_grp_df$vid_name <-
        cur_vid_names_list
      
    } else if ("matrix" == class(vid_time_rng_grp_df[, "vid"])) {
      cur_vid_grp_mat <- vid_time_rng_grp_df[, "vid"]
      cur_grp_vid_name_mat <-
        matrix(ncol = ncol(cur_vid_grp_mat),
               nrow = nrow(cur_vid_grp_mat))
      for (row in 1:nrow(cur_vid_grp_mat)) {
        for (col in 1:ncol(cur_vid_grp_mat)) {
          cur_vid <- cur_vid_grp_mat[row, col]
          # print(class(cur_vid))
          cur_vid_name <-
            vid_time_info_df[which(vid_time_info_df$vid == cur_vid), "vid_name"]
          cur_grp_vid_name_mat[row, col] <- cur_vid_name
        }
      }
      vid_time_rng_grp_df$vid_name <- cur_grp_vid_name_mat
    } else {
      cur_vid_vec <- vid_time_rng_grp_df[, "vid"]
      cur_grp_vid_name_vec <- cur_vid_vec
      for (cur_vid_i in seq_along(cur_vid_vec)) {
        # print(class(cur_vid))
        cur_vid <- cur_vid_vec[cur_vid_i]
        cur_vid_name <-
          vid_time_info_df[which(vid_time_info_df$vid == cur_vid), "vid_name"]
        cur_grp_vid_name_vec[cur_vid_i] <- cur_vid_name
      }
      vid_time_rng_grp_df$vid_name <- cur_grp_vid_name_vec
    }
    
    
    # loads the PostgreSQL driver
    hourly.vv.pg.drv <- dbDriver("PostgreSQL")
    # create a connection
    # save the password that we can "hide" it as best as we can by collapsing it
    pw <-
      "Dqi2Si3EER_ja983KBDFg"
    
    # creates a connection to the postgres database
    # note that "con" will be used later in each connection to the database
    hourly.vv.pg.conn <- dbConnect(
      hourly.vv.pg.drv,
      dbname = "dm_pv_fact",
      host = "10.27.106.88",
      port = 2345,
      user = "analyse_readonly",
      password = pw
    )
    remove(pw)
    
    # cur_vid_sql_cond_str <- "4305096, 4305053"
    # cur_start_date_str <- "20180307"
    # cur_end_date_str <- "20180312"
    
    cur_hourly_vv_df <- NULL
    vid_sql_cnt <- 0
    for (vid_grp_i in 1:nrow(vid_time_rng_grp_df)) {
      if (vid_grp_i > mru_vid_grp_cnt)
        break
      cur_vid_str_list <-
        unlist(vid_time_rng_grp_df[vid_grp_i, "vid"])
      cur_vid_name_list <-
        unlist(vid_time_rng_grp_df[vid_grp_i, "vid_name"])
      vid_sql_cnt <-
        vid_sql_cnt + length(cur_vid_str_list)
      cur_vid_sql_cond_str <-
        paste(cur_vid_str_list, collapse = ",")
      cur_vid_name_str <- paste0(cur_vid_name_list, collapse = ",")
      cur_start_date <-
        vid_time_rng_grp_df[vid_grp_i, "start_date"]
      cur_start_time <- vid_time_rng_grp_df[vid_grp_i, "start_time"]
      cur_start_time <-
        strptime(strftime(cur_start_time, format = "%Y-%m-%d %H"), format = "%Y-%m-%d %H") # remove minute and seconds
      cur_start_date_str <-
        strftime(cur_start_date, format = "%Y%m%d")
      cur_end_date <-
        vid_time_rng_grp_df[vid_grp_i, "end_date"]
      cur_end_date_str <-
        strftime(cur_end_date, format = "%Y%m%d")
      
      cur_vid_query_date = cur_start_date
      hourly.vv.start.time <- Sys.time()
      while (cur_vid_query_date <= cur_end_date) {
        cur_date_str <- format(cur_vid_query_date, "%Y%m%d")
        vid_hourly_vv_sql = paste0(
          c(
            sprintf(
              "select year, month, day, hour, vid, count(*) as vv from vv_fact_%s_%s where",
              cur_bid_name,
              cur_date_str
            ),
            # sprintf("bid in (%s) and", cur_bid_str),
            sprintf("vid in (%s)",
                    cur_vid_sql_cond_str),
            # sprintf(
            #   "year||month||day between '%s' and '%s' and vid in (%s)",
            #   cur_start_date_str,
            #   cur_end_date_str,
            #   cur_vid_sql_cond_str
            # ),
            "group by year, month, day, hour, vid",
            "order by year, month, day, hour, vv DESC;"
          ),
          collapse = " "
        )
        cur_vv_valid <- TRUE
        cur_vid_vv_file <-
          sprintf(
            "%s/%s_%s_%s",
            vv_file_dir,
            cur_bid_str,
            cur_vid_sql_cond_str,
            cur_date_str
          )
        cur_vid_vv_file_info <- file.info(cur_vid_vv_file)
        if (!is.na(cur_vid_vv_file_info) &&
            cur_vid_vv_file_info$size != 0) {
          cur_date_hourly_vv_df <- dget(cur_vid_vv_file)
        } else {
          # print(vid_hourly_vv_sql)
          cur.fetch.na <- FALSE
          tryCatch({
            cur_date_hourly_vv_df <-
              dbGetQuery(hourly.vv.pg.conn, vid_hourly_vv_sql)
          }, error = function(e) {
            error(logger, as.character(e))
            print(sys.calls())
            cur.fetch.na <<- TRUE
          })
          if (!cur.fetch.na) {
            if (nrow(cur_date_hourly_vv_df)) {
              cur_date_hourly_vv_df$time <-
                strptime(
                  sprintf(
                    "%d-%s-%s %s",
                    cur_date_hourly_vv_df$year,
                    cur_date_hourly_vv_df$month,
                    cur_date_hourly_vv_df$day,
                    cur_date_hourly_vv_df$hour
                  ),
                  format = "%Y-%m-%d %H"
                )
              cur_date_hourly_vv_df$day_since_release <-
                cur_vid_query_date - cur_start_date + 1
              cur_date_hourly_vv_df$hour_since_release <-
                as.numeric(cur_date_hourly_vv_df$time - cur_start_time,
                           units = "hours") + 1
              cur_date_hourly_vv_df_m <-
                merge(cur_date_hourly_vv_df, vid_time_info_df)
              cur_date_hourly_vv_df <-
                cur_date_hourly_vv_df_m[, c(
                  "vid",
                  "vv",
                  "serialno",
                  "time",
                  "day_since_release",
                  "hour_since_release",
                  "vid_name"
                )]
            } else{
              warn(
                logger,
                sprintf(
                  "%s(%s) %s-%s hourly VV is EMPTY\n\t%s",
                  cur_vid_sql_cond_str,
                  cur_vid_name_str,
                  cur_start_date_str,
                  cur_end_date_str,
                  vid_hourly_vv_sql
                )
              )
              sink(emp_sql_file)
              cat(vid_hourly_vv_sql)
              sink()
              cur_vv_valid <- FALSE
            }
          } else {
            cur_vv_valid <- FALSE
          }
        }
        if (cur_vv_valid) {
          if (any(with(cur_date_hourly_vv_df, hour_since_release <= 0))) {
            err_out_str <-
              sprintf("%s\n%s",
                      cur_out_comm_prefix,
                      capture.output(
                        subset(cur_date_hourly_vv_df, hour_since_release <= 0)
                      ))
            warn(logger,
                 err_out_str)
            sink(err_out_file)
            cat(err_out_str)
            sink()
            # potential internal testing VV before online
            cur_date_hourly_vv_df <-
              subset(cur_date_hourly_vv_df, hour_since_release > 0)
          }
          dput(cur_date_hourly_vv_df, file = cur_vid_vv_file)
          
          cur_hourly_vv_df <-
            rbind(cur_hourly_vv_df, cur_date_hourly_vv_df)
        }
        cur_vid_query_date = cur_vid_query_date + 1
        if (!cur_vv_valid &&
            cur_vid_query_date <= cur_end_date) {
          cur_start_date_str <- format(cur_vid_query_date, "%Y%m%d")
        }
      } # while current vid group's hourly query date not end
      hourly.vv.end.time <- Sys.time()
      hourly.vv.time.taken <-
        hourly.vv.end.time - hourly.vv.start.time
      
      cur_task_cnt <- (j - 1) * top_pid_cnt + i
      cur_pid_days_count = cur_end_date - cur_start_date + 1
      info(
        logger,
        sprintf(
          "%d(%.2f%%).%d(%.2f%%)\t%s 正片'%s' 查询%d天逐小时VV耗时:%s",
          i,
          (cur_task_cnt * 1.0) / total_task_num * 100,
          vid_grp_i,
          (vid_grp_i * 1.0) /  mru_vid_grp_cnt * 100,
          cur_out_comm_prefix,
          cur_vid_name_str,
          cur_pid_days_count,
          format(hourly.vv.time.taken)
        )
      )
    } # for every vid group of the same release time
    
    stopifnot(all(with(
      cur_hourly_vv_df, hour_since_release > 0
    )))
    
    cur_hourly_vv_df <-
      cur_hourly_vv_df[with(cur_hourly_vv_df, order(-serialno, hour_since_release)), ]
    
    # shorten plot name
    cur_hourly_vv_df$vid_plot_name <-
      gsub(cur_pid_name, "", cur_hourly_vv_df$vid_name, fixed = T)
    pat <- "^[^0-9第]*(第?[0-9]+[期集]).*$"
    cur_hourly_vv_df$vid_plot_name <-
      sub(pat, "\\1", cur_hourly_vv_df$vid_plot_name, perl = T)
    pat <- "([^:：]*)\\s*.*$" # remove long sub title for plotting
    cur_hourly_vv_df$vid_plot_name <-
      sub(pat, "\\1", cur_hourly_vv_df$vid_plot_name, perl = T)
    
    # parpare trainning sets
    episodes <-
      unique(cur_hourly_vv_df$serialno) # ordered already
    episode_cnt <- length(episodes)
    cur_training_xreg <- NULL
    if (episode_cnt >= 3) {
      # min_len <- Inf
      # for (episode in episodes) {
      #   cur_episode_hourly_vv_df <-
      #     cur_hourly_vv_df[which(cur_hourly_vv_df$serialno == episode),]
      #   if (length(cur_episode_hourly_vv_df$hour_since_release) < min_len) {
      #     min_len <- length(cur_episode_hourly_vv_df$hour_since_release)
      #   }
      # }
      cur_vv_idx_rng <- c(Inf, -Inf)
      for (episode in episodes) {
        cur_episode_hourly_vv_df <-
          cur_hourly_vv_df[which(cur_hourly_vv_df$serialno == episode), ]
        cur_episode_idx_rng <-
          range(cur_episode_hourly_vv_df$hour_since_release)
        if (cur_episode_idx_rng[1] < cur_vv_idx_rng[1]) {
          cur_vv_idx_rng[1] <- cur_episode_idx_rng[1]
        }
        if (cur_episode_idx_rng[2] > cur_vv_idx_rng[2]) {
          cur_vv_idx_rng[2] <- cur_episode_idx_rng[2]
        }
      }
      cur_x_idx <-
        seq(from = cur_vv_idx_rng[1], to = cur_vv_idx_rng[2])
      xreg <- NULL
      for (episode in episodes) {
        cur_vv_vec <- rep(NA, length(cur_x_idx))
        cur_episode_hourly_vv_df <-
          cur_hourly_vv_df[which(cur_hourly_vv_df$serialno == episode),]
        stopifnot(length(unique(
          cur_episode_hourly_vv_df$vid_plot_name
        )) == 1)
        cur_vid_plot_name <-
          unique(cur_episode_hourly_vv_df$vid_plot_name)
        
        # cur_episode_train_info_df <-
        #   cur_episode_hourly_vv_df[1:min_len, ]
        # cur_xreg <- cur_episode_train_info_df$vv
        
        cur_vv_vec[cur_episode_hourly_vv_df$hour_since_release] <-
          cur_episode_hourly_vv_df$vv
        cur_xreg <- cur_vv_vec
        # names(cur_xreg) <-
        #   cur_episode_train_info_df$hour_since_release
        xreg <-
          cbind(xreg, cur_xreg) # combine vector of DIFFERENT length
        colnames(xreg)[ncol(xreg)] <- cur_vid_plot_name
      }
      
      # remove column with less than 3 non-na values(just released video)
      xreg_non_na_colSums <- colSums(!is.na(xreg))
      if (any(xreg_non_na_colSums < 3)) {
        warn(
          logger,
          sprintf(
            "%s %s is dropped due to too few data.",
            cur_out_comm_prefix,
            colnames(xreg[, xreg_non_na_colSums < 3, drop = FALSE])
          )
        )
      }
      xreg <- xreg[, xreg_non_na_colSums >= 3]
      
      # hour_since_release calibration
      # xreg_all_na_rows <-
      #   xreg[rowSums(is.na(xreg)) == ncol(xreg), , drop = FALSE]
      xreg_all_na_row_idx <-
        which(rowSums(is.na(xreg)) == ncol(xreg))
      xreg_all_na_row_idx_rng <- range(xreg_all_na_row_idx)
      all_na_at_head <- xreg_all_na_row_idx_rng[[1]] == 1
      if (all_na_at_head) {
        xreg_all_na_head_row_idx <-
          which(cumsum(c(1, diff(
            xreg_all_na_row_idx
          ) != 1)) == 1)  # https://stackoverflow.com/questions/23095415/how-to-find-if-the-numbers-are-continuous-in-r#23095527
        xreg_all_na_head_row_idx_rng <-
          range(xreg_all_na_head_row_idx)
        stopifnot(
          length(xreg_all_na_head_row_idx) == (
            xreg_all_na_head_row_idx_rng[[2]] - xreg_all_na_head_row_idx_rng[[1]] + 1
          )
        )
        if (length(xreg_all_na_head_row_idx)) {
          # xreg_all_na_head_row_idx <-
          #   as.integer(dimnames(xreg_all_na_rows)[[1]])  # not all xreg has dimnames[[1]]
          # xreg_all_na_head_row_max_idx <-
          #   xreg_all_na_head_row_idx[length(xreg_all_na_head_row_idx)]
          # xreg_orig_row_idx <-
          #   as.integer(dimnames(xreg)[[1]])
          xreg_all_na_head_row_max_idx <-
            xreg_all_na_head_row_idx[length(xreg_all_na_head_row_idx)]
          has_row_names <- FALSE
          if (!is.null(dimnames(xreg)[[1]])) {
            has_row_names <- TRUE
          }
          if (has_row_names) {
            xreg_orig_row_idx <-
              as.integer(dimnames(xreg)[[1]])
          }
          xreg <-
            xreg[which(rowSums(is.na(xreg)) != ncol(xreg)), , drop = FALSE]
          
          if (has_row_names) {
            if (length(xreg_all_na_head_row_idx) &&
                length(cur_x_idx) != nrow(xreg)) {
              cur_x_idx <- cur_x_idx[-xreg_all_na_head_row_idx]
            }
            dim_names <- dimnames(xreg)
            dimnames(xreg) <-
              list(cur_x_idx, dim_names[[2]]) # add original row index
          }
          
          cur_hourly_vv_df$hour_since_release <-
            cur_hourly_vv_df$hour_since_release - xreg_all_na_head_row_max_idx
          cur_hourly_vv_df <-
            subset(cur_hourly_vv_df, hour_since_release > 0)
        }
      }
    } else{
      warn(
        logger,
        sprintf(
          "%s has only %d episodes, skipped.",
          cur_out_comm_prefix,
          episode_cnt
        )
      )
    }
    
    if (nrow(cur_hourly_vv_df)) {
      require("scales")
      cur_hourly_vv_plot_df <-
        subset(cur_hourly_vv_df,
               hour_since_release <= max_day_time_span * 24)
      x_rng <- range(cur_hourly_vv_plot_df$hour_since_release)
      x_breaks <- seq(x_rng[1], x_rng[2], by = 4)
      p <-
        ggplot(data = cur_hourly_vv_plot_df,
               aes(
                 x = hour_since_release,
                 y = vv,
                 color = vid_plot_name,
                 group = serialno
               )) +
        geom_point() +
        geom_line(aes(linetype = vid_plot_name)) +
        scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
        theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
        
        ylab("每小时VV") +
        ggtitle(sprintf("%s Hourly VV", cur_out_comm_prefix))
      # print(p)
      ggsave(
        sprintf(
          "%s/%s_%s_vid_hourly_vv.png",
          png_file_dir,
          cur_bid_name,
          cur_pid_name
        ),
        width = 12,
        height = 6
      )
      
      pat <- "([^:：]*)\\s*.*$" # remove long sub title for plotting
      cur_hourly_vv_plot_df$facet_lab <-
        gsub(pat, "\\1", cur_hourly_vv_plot_df$vid_plot_name)
      ggplot(data = cur_hourly_vv_plot_df,
             aes(x = hour_since_release, y = vv, color = vid_plot_name)) +
        facet_grid(facet_lab ~ .) +
        geom_point() +
        geom_line(aes(linetype = vid_plot_name)) +
        scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
        theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
        theme(legend.position = "bottom") +
        xlab("上线小时数") +
        ylab("每小时VV") +
        ggtitle(sprintf("%s Hourly VV", cur_out_comm_prefix))
      ggsave(
        sprintf(
          "%s/%s_%s_vid_facet_hourly_vv.png",
          png_file_dir,
          cur_bid_name,
          cur_pid_name
        ),
        width = 12,
        height = 6
      )
      # vid_daily_vv_sql = paste0(c(
      #   sprintf(
      #     "select date, vid, sum(vv) as vv from vv_vid_day where bid in (%s) and ",
      #     cur_bid_str
      #   ),
      #   sprintf(
      #     "date between %s and %s and vid != -1 and ",
      #     cur_start_date_str,
      #     cur_end_date_str
      #   ),
      #   sprintf(
      #     "vid in (%s) group by date, vid order by date, vv desc",
      #     cur_vid_sql_cond_str
      #   )
      # ))
    }
    
    # how far newest episode is from previous episodes average
    xreg_ncol <- ncol(xreg)
    if (ncol(xreg[,-1, drop = FALSE]) > 1) {
      prev_mean <- rowMeans(xreg[,-1], na.rm = TRUE)
    } else {
      prev_mean <- xreg[,-1]
    }
    # remove continuous NA values at tail to get the error metrics
    prev_mean_na_row_idx <- which(is.na(prev_mean))
    if (length(prev_mean_na_row_idx)) {
      prev_mean_contin_rng_idx_list <-
        unname(tapply(prev_mean_na_row_idx, cumsum(c(
          TRUE, diff(prev_mean_na_row_idx) != 1
        )), range))
      prev_mean_na_tail_row_rng_idx <-
        prev_mean_contin_rng_idx_list[[length(prev_mean_contin_rng_idx_list)]]
      prev_mean_na_tail_row_idx <-
        seq(from = prev_mean_na_tail_row_rng_idx[[1]], to = prev_mean_na_tail_row_rng_idx[[2]])
      prev_mean <- prev_mean[-prev_mean_na_tail_row_idx]
    }
    
    cur_vv <- xreg[, 1, drop = FALSE]
    cur_vv_orig <- cur_vv
    cur_vv_na_row_idx <- which(rowSums(is.na(cur_vv)) > 0)
    cur_vv_na_idx <- NULL
    if (length(cur_vv_na_row_idx)) {
      # remove continuous NA value at tail to avoid interpolation
      cur_vv_contin_rng_idx_list <-
        unname(tapply(cur_vv_na_row_idx, cumsum(c(
          TRUE, diff(cur_vv_na_row_idx) != 1
        )), range))
      cur_vv_na_tail_row_rng_idx <-
        cur_vv_contin_rng_idx_list[[length(cur_vv_contin_rng_idx_list)]]
      cur_vv_na_tail_row_idx <-
        seq(from = cur_vv_na_tail_row_rng_idx[[1]], to = cur_vv_na_tail_row_rng_idx[[2]])
      cur_vv <- cur_vv[-cur_vv_na_tail_row_idx]
      require("imputeTS")
      cur_vv_imput <-
        na.kalman(cur_vv)  # fill intermitten missing value to get the error metrics value
      cur_vv_na_idx <- which(is.na(cur_vv))
      cur_vv <- cur_vv_imput
    }
    require("Metrics")
    cur_mape <- mape(cur_vv, prev_mean)
    names(cur_mape) <- cur_pid_str
    mape_list[[cur_bid_str]] <-
      c(mape_list[[cur_bid_str]], list(cur_mape))
    cur_rmse <- rmse(cur_vv, prev_mean)
    names(cur_rmse) <- cur_pid_str
    rmse_list[[cur_bid_str]] <-
      c(rmse_list[[cur_bid_str]], list(cur_rmse))
    cur_vv_lab <- colnames(xreg[, 1, drop = FALSE])
    cur_vv_imput_orig <- cur_vv
    prev_mean_orig <- prev_mean
    if (length(cur_vv) != length(prev_mean)) {
      if (length(cur_vv) < length(prev_mean)) {
        cur_vv <-
          rep(NA, length(prev_mean))  # add back the na values at tail to align length again for cbind
        cur_vv[1:length(cur_vv_imput_orig)] <- cur_vv_imput_orig
      } else if (length(cur_vv) > length(prev_mean)) {
        prev_mean <- rep(NA, length(cur_vv))
        prev_mean[1:length(prev_mean_orig)] <- prev_mean_orig
      }
    }
    episode_comp_df <- cbind(cur_vv, prev_mean)
    colnames(episode_comp_df) <-
      c(cur_vv_lab, sprintf("预测值"))
      # c(cur_vv_lab, sprintf("前%d集均值", ncol(xreg[, -1, drop = FALSE])))
    episode_comp_plot_df <- melt(episode_comp_df)
    colnames(episode_comp_plot_df) <- c("x", "legend_lab", "y")
    episode_comp_plot_df$plot_lab <-
      as.character(episode_comp_plot_df$legend_lab)
    if (length(cur_vv_na_idx)) {
      # this should still be valid index after the x-axis cutoff
      episode_comp_plot_df[with(episode_comp_plot_df,
                                which(legend_lab == cur_vv_lab &
                                        x %in% cur_vv_na_idx)), "plot_lab"] <-
        sprintf("%s插值", cur_vv_lab)
    }
    x_cutoff_val <-
      min(length(cur_vv_imput_orig), length(prev_mean_orig))  # record the real x-axis cutoff value to display  lines of same length
    episode_comp_plot_df <-
      subset(episode_comp_plot_df, !is.na(y) &
               x <= x_cutoff_val)
    x_breaks_rng <- range(episode_comp_plot_df$x)
    x_breaks <- seq(x_breaks_rng[[1]], x_breaks_rng[[2]], by = 4)
    ggplot(episode_comp_plot_df, aes(x = x, y = y, color = plot_lab)) +
      geom_point() +
      geom_line(aes(linetype = plot_lab)) +
      theme(legend.position = "bottom") +
      scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
      theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
      xlab("上线时间") +
      ylab("每小时VV") +
      ggtitle(sprintf(
        "%s MAPE = %.4f, RMSE = %.4f",
        cur_out_comm_prefix,
        cur_mape,
        cur_rmse
      ))
    ggsave(
      sprintf(
        "%s/%s_%s_cur-prev_comp.png",
        png_file_dir,
        cur_bid_name,
        cur_pid_name
      ),
      width = 12,
      height = 6
    )
    
    trailer_type = 3
    # get trailers video and VV info
    trailer_info_sql <- paste0(
      c(
        "select a.partId as vid, a.partName as vid_name, b.fstlvlType, a.serialno, a.clipId,",
        "b.clipName, b.playTime, b.newPartUpTime, a.mppOnlineTime, a.updateTime, a.createTime, a.releaseTime from (",
        "select partId, partName, serialno, clipId, mppOnlineTime, updateTime, createTime, releaseTime from asset_v_parts",
        sprintf("where clipId in (%s) and", cur_pid_str),
        sprintf("isIntact = %d", trailer_type),
        ") a",
        "inner join asset_v_clips b on a.clipId = b.clipId order by mppOnlineTime DESC, serialno DESC;"
      ),
      collapse = " "
    )
    print(trailer_info_sql)
    trailer_time_info_df <- dbGetQuery(mysql.conn, trailer_info_sql)
    trailer_time_info_df$newPartUpTime <-
      strptime(trailer_time_info_df$newPartUpTime, "%Y-%m-%d %H:%M:%S")
    trailer_time_info_df$updateTime <-
      strptime(trailer_time_info_df$updateTime, "%Y-%m-%d %H:%M:%S")
    trailer_time_info_df$createTime <-
      strptime(trailer_time_info_df$createTime, "%Y-%m-%d %H:%M:%S")
    trailer_time_info_df$releaseTime <-
      strptime(trailer_time_info_df$mppOnlineTime, "%Y-%m-%d %H:%M:%S") # POSIXlt
    trailer_time_info_df <-
      subset(trailer_time_info_df,!is.na(releaseTime))
    
    # check for serialno continunity
    trailer_uniq_serialno <- unique(trailer_time_info_df$serialno)
    trailer_serial_no_rng <- range(trailer_uniq_serialno)
    if (length(trailer_uniq_serialno) != (trailer_serial_no_rng[[2]] - trailer_serial_no_rng[[1]] + 1)) {
      warn(
        logger,
        sprintf(
          "%s '%s' serailno field is discontinuous, skipped.\n\t%s",
          cur_bid_name,
          cur_pid_name,
          trailer_info_sql
        )
      )
      serialno_missing_pid_list[[cur_bid_str]] <-
        c(serialno_missing_pid_list[[cur_bid_str]], list(cur_pid_str))
      skipped_pid_list[[cur_bid_str]] <-
        c(skipped_pid_list[[cur_bid_str]], list(cur_pid_str))
      next
    }
    
    trailer_time_rng_df <- NULL
    uniq_trailer_rel_time <-
      unique(trailer_time_info_df$releaseTime)
    for (row_i in 1:nrow(trailer_time_info_df)) {
      cur_vid = trailer_time_info_df[row_i, "vid"]
      cur_trailer_name = trailer_time_info_df[row_i, "vid_name"]
      cur_serial_no <- trailer_time_info_df[row_i, "serialno"]
      cur_rel_time <- trailer_time_info_df[row_i, "releaseTime"]
      cur_rel_time_level_idx <-
        which(uniq_trailer_rel_time == cur_rel_time)
      maximum_allow_time <-
        strptime(sprintf("%s 00:00:00", as.character(yesterday)), format = "%Y-%m-%d %H:%M:%S")
      if (cur_rel_time > maximum_allow_time) {
        cur_rel_time <- maximum_allow_time
      }
      cur_rel_date <-
        as.Date(cur_rel_time)
      if (cur_rel_date > yesterday) {
        cur_rel_date <- yesterday
      }
      if (1 == cur_rel_time_level_idx) {
        cur_real_end_date <- cur_rel_date + max_day_time_span
        cur_real_end_time <-
          strptime(sprintf("%s 23:59:59", cur_real_end_date), format = "%Y-%m-%d %H:%M:%S")
      } else{
        cur_real_end_date <-
          as.Date(uniq_trailer_rel_time[cur_rel_time_level_idx - 1])
        cur_real_end_time <-
          uniq_trailer_rel_time[cur_rel_time_level_idx - 1]
      }
      if (cur_real_end_date > yesterday) {
        cur_real_end_date <- yesterday
        cur_real_end_time <- maximum_allow_time
      }
      trailer_time_rng_df <-
        rbind(
          trailer_time_rng_df,
          data.frame(
            vid = cur_vid,
            vid_name = cur_trailer_name,
            serialno = cur_serial_no,
            start_date = cur_rel_date,
            end_date = cur_real_end_date,
            start_time = cur_rel_time,
            end_time = cur_real_end_time
          )
        )
    } # for every latest vid(s)
    trailer_time_rng_df$vid_name <-
      as.character(trailer_time_rng_df$vid_name)
    # merge similar release time vid
    for (vid_i in seq(2, nrow(trailer_time_rng_df))) {
      # time decreasing order
      if (as.numeric(trailer_time_rng_df[vid_i - 1, "start_time"] - trailer_time_rng_df[vid_i, "start_time"], units = "secs") < 60) {
        trailer_time_rng_df[vid_i - 1, "start_time"] <-
          trailer_time_rng_df[vid_i, "start_time"]
      }
      if (as.numeric(trailer_time_rng_df[vid_i - 1, "end_time"] - trailer_time_rng_df[vid_i, "end_time"], units = "secs") < 60) {
        trailer_time_rng_df[vid_i - 1, "end_time"] <-
          trailer_time_rng_df[vid_i, "end_time"]
      }
    }
    
    trailer_time_rng_grp_df <-
      aggregate(vid ~ start_time + start_date + end_date + end_time,
                data = trailer_time_rng_df,
                FUN = c)  # this change the date order of trailer_time_rng_df, and the factor names is lost when formaulat is c(factor1, factor2) ~ grp_var1 + grp_var2
    trailer_time_rng_grp_df <-
      trailer_time_rng_grp_df[with(trailer_time_rng_grp_df,
                                   order(end_time,
                                         start_time,
                                         decreasing = T)), ]
    # append vid name
    if ("list" == class(trailer_time_rng_grp_df[, "vid"])) {
      cur_trailer_grp_list <-
        as.list(trailer_time_rng_grp_df[, "vid"]) # list OR matrix
      cur_trailer_names_list <- c(c())
      for (elem_i in seq_along(cur_trailer_grp_list)) {
        cur_trailer_name_list <- c()
        for (col_j in seq_along(cur_trailer_grp_list[[elem_i]])) {
          cur_vid <- cur_trailer_grp_list[[elem_i]][[col_j]]
          # print(class(cur_vid))
          cur_trailer_name <-
            trailer_time_info_df[which(trailer_time_info_df$vid == cur_vid), "vid_name"]
          # print(cur_trailer_name)
          cur_trailer_name_list <-
            c(cur_trailer_name_list, cur_trailer_name)
        }
        cur_trailer_names_list <-
          c(cur_trailer_names_list, list(cur_trailer_name_list))
      }
      
      trailer_time_rng_grp_df$vid_name <-
        cur_trailer_names_list
      
    } else if ("matrix" == class(trailer_time_rng_grp_df[, "vid"])) {
      cur_trailer_grp_mat <- trailer_time_rng_grp_df[, "vid"]
      cur_grp_trailer_name_mat <-
        matrix(
          ncol = ncol(cur_trailer_grp_mat),
          nrow = nrow(cur_trailer_grp_mat)
        )
      for (row in 1:nrow(cur_trailer_grp_mat)) {
        for (col in 1:ncol(cur_trailer_grp_mat)) {
          cur_vid <- cur_trailer_grp_mat[row, col]
          # print(class(cur_vid))
          cur_trailer_name <-
            trailer_time_info_df[which(trailer_time_info_df$vid == cur_vid), "vid_name"]
          cur_grp_trailer_name_mat[row, col] <- cur_trailer_name
        }
      }
      trailer_time_rng_grp_df$vid_name <-
        cur_grp_trailer_name_mat
    } else {
      cur_vid_vec <- trailer_time_rng_grp_df[, "vid"]
      cur_grp_trailer_name_vec <- cur_vid_vec
      for (cur_trailer_i in seq_along(cur_vid_vec)) {
        # print(class(cur_vid))
        cur_vid <- cur_vid_vec[cur_trailer_i]
        cur_trailer_name <-
          trailer_time_info_df[which(trailer_time_info_df$vid == cur_vid), "vid_name"]
        cur_grp_trailer_name_vec[cur_trailer_i] <- cur_trailer_name
      }
      trailer_time_rng_grp_df$vid_name <-
        cur_grp_trailer_name_vec
    }
    
    cur_trail_hourly_vv_df <- NULL
    vid_sql_cnt <- 0
    for (vid_grp_i in 1:nrow(trailer_time_rng_grp_df)) {
      if (vid_grp_i > mru_vid_grp_cnt)
        break
      cur_vid_str_list <-
        unlist(trailer_time_rng_grp_df[vid_grp_i, "vid"])
      cur_vid_name_list <-
        unlist(trailer_time_rng_grp_df[vid_grp_i, "vid_name"])
      vid_sql_cnt <-
        vid_sql_cnt + length(cur_vid_str_list)
      cur_vid_sql_cond_str <-
        paste(cur_vid_str_list, collapse = ",")
      cur_vid_name_str <- paste0(cur_vid_name_list, collapse = ",")
      cur_start_date <-
        trailer_time_rng_grp_df[vid_grp_i, "start_date"]
      cur_start_time <-
        trailer_time_rng_grp_df[vid_grp_i, "start_time"]
      cur_start_time <-
        strptime(strftime(cur_start_time, format = "%Y-%m-%d %H"), format = "%Y-%m-%d %H") # remove minute and seconds
      cur_start_date_str <-
        strftime(cur_start_date, format = "%Y%m%d")
      cur_end_date <-
        trailer_time_rng_grp_df[vid_grp_i, "end_date"]
      cur_end_date_str <-
        strftime(cur_end_date, format = "%Y%m%d")
      
      cur_vid_query_date = cur_start_date
      hourly.vv.start.time <- Sys.time()
      while (cur_vid_query_date <= cur_end_date) {
        cur_date_str <- format(cur_vid_query_date, "%Y%m%d")
        vid_hourly_vv_sql = paste0(
          c(
            sprintf(
              "select year, month, day, hour, vid, count(*) as vv from vv_fact_%s_%s where",
              cur_bid_name,
              cur_date_str
            ),
            # sprintf("bid in (%s) and", cur_bid_str),
            sprintf("vid in (%s)",
                    cur_vid_sql_cond_str),
            # sprintf(
            #   "year||month||day between '%s' and '%s' and vid in (%s)",
            #   cur_start_date_str,
            #   cur_end_date_str,
            #   cur_vid_sql_cond_str
            # ),
            "group by year, month, day, hour, vid",
            "order by year, month, day, hour, vv DESC;"
          ),
          collapse = " "
        )
        cur_vv_valid <- TRUE
        cur_vid_vv_file <-
          sprintf(
            "%s/%s_%s_%s",
            vv_file_dir,
            cur_bid_str,
            cur_vid_sql_cond_str,
            cur_date_str
          )
        cur_vid_vv_file_info <- file.info(cur_vid_vv_file)
        if (!is.na(cur_vid_vv_file_info) &&
            cur_vid_vv_file_info$size != 0) {
          cur_trail_date_hourly_vv_df <- dget(cur_vid_vv_file)
        } else {
          # print(vid_hourly_vv_sql)
          cur.fetch.na <- FALSE
          tryCatch({
            cur_trail_date_hourly_vv_df <-
              dbGetQuery(hourly.vv.pg.conn, vid_hourly_vv_sql)
          }, error = function(e) {
            error(logger, as.character(e))
            print(sys.calls())
            cur.fetch.na <<- TRUE
          })
          if (!cur.fetch.na) {
            if (nrow(cur_trail_date_hourly_vv_df)) {
              cur_trail_date_hourly_vv_df$time <-
                strptime(
                  sprintf(
                    "%d-%s-%s %s",
                    cur_trail_date_hourly_vv_df$year,
                    cur_trail_date_hourly_vv_df$month,
                    cur_trail_date_hourly_vv_df$day,
                    cur_trail_date_hourly_vv_df$hour
                  ),
                  format = "%Y-%m-%d %H"
                )
              cur_trail_date_hourly_vv_df$day_since_release <-
                cur_vid_query_date - cur_start_date + 1
              cur_trail_date_hourly_vv_df$hour_since_release <-
                as.numeric(cur_trail_date_hourly_vv_df$time - cur_start_time,
                           units = "hours") + 1
              cur_date_hourly_vv_df_m <-
                merge(cur_trail_date_hourly_vv_df,
                      trailer_time_info_df)
              cur_trail_date_hourly_vv_df <-
                cur_date_hourly_vv_df_m[, c(
                  "vid",
                  "vv",
                  "serialno",
                  "time",
                  "day_since_release",
                  "hour_since_release",
                  "vid_name"
                )]
            } else{
              warn(
                logger,
                sprintf(
                  "%s %s(%s) %s-%s hourly VV is EMPTY\n\t%s",
                  cur_bid_name,
                  cur_vid_sql_cond_str,
                  cur_vid_name_str,
                  cur_start_date_str,
                  cur_end_date_str,
                  vid_hourly_vv_sql
                )
              )
              sink(emp_sql_file)
              cat(vid_hourly_vv_sql)
              sink()
              cur_vv_valid <- FALSE
            }
          } else {
            cur_vv_valid <- FALSE
          }
        }
        if (cur_vv_valid) {
          if (any(with(
            cur_trail_date_hourly_vv_df,
            hour_since_release <= 0
          ))) {
            err_out_str <-
              sprintf("%s\n%s",
                      cur_out_comm_prefix,
                      capture.output(
                        subset(
                          cur_trail_date_hourly_vv_df,
                          hour_since_release <= 0
                        )
                      ))
            warn(logger,
                 err_out_str)
            sink(err_out_file)
            cat(err_out_str)
            sink()
            # potential internal testing VV before online
            cur_trail_date_hourly_vv_df <-
              subset(cur_trail_date_hourly_vv_df,
                     hour_since_release > 0)
          }
          dput(cur_trail_date_hourly_vv_df, file = cur_vid_vv_file)
          cur_trail_hourly_vv_df <-
            rbind(cur_trail_hourly_vv_df,
                  cur_trail_date_hourly_vv_df)
        }
        cur_vid_query_date = cur_vid_query_date + 1
        if (!cur_vv_valid &&
            cur_vid_query_date <= cur_end_date) {
          cur_start_date_str <- format(cur_vid_query_date, "%Y%m%d")
        }
        
      } # while current vid group's hourly query date not end
      hourly.vv.end.time <- Sys.time()
      hourly.vv.time.taken <-
        hourly.vv.end.time - hourly.vv.start.time
      
      cur_task_cnt <- (j - 1) * top_pid_cnt + i
      cur_pid_days_count = cur_end_date - cur_start_date + 1
      info(
        logger,
        sprintf(
          "%d(%.2f%%).%d(%.2f%%)\t%s 预告片'%s' 查询%d天逐小时VV耗时:%s",
          i,
          (cur_task_cnt * 1.0) / total_task_num * 100,
          vid_grp_i,
          (vid_grp_i * 1.0) / mru_vid_grp_cnt * 100,
          cur_out_comm_prefix,
          cur_vid_name_str,
          cur_pid_days_count,
          format(hourly.vv.time.taken)
        )
      )
    } # for every vid group of the same release time
    
    # shorten plot name
    pat <- sprintf("《?%s》?", cur_pid_name)
    cur_trail_hourly_vv_df$vid_plot_name <-
      gsub(pat,
           "",
           cur_trail_hourly_vv_df$vid_name,
           perl = T)  # remove common TV program/clip name
    # # one full-length may have multiple trailer with SAME main title and different sub title/description
    # pat <- "^[^0-9第]*(第?[0-9]+[期集]).*$"
    # cur_trail_hourly_vv_df$vid_plot_name <-
    #   sub(pat, "\\1", cur_trail_hourly_vv_df$vid_name, perl = T)  # keep main title only
    # pat <- "([^:：]*)\\s*.*$"  # remove long sub title for plotting
    # cur_trail_hourly_vv_df$vid_plot_name <-
    #   sub(pat, "\\1", cur_trail_hourly_vv_df$vid_plot_name, perl = T)
    
    
    cur_trail_hourly_vv_df <-
      cur_trail_hourly_vv_df[with(cur_trail_hourly_vv_df,
                                  order(-serialno, hour_since_release)), ]
    # parpare trainning sets
    episodes <-
      unique(cur_trail_hourly_vv_df$serialno) # ordered already
    episode_cnt <- length(episodes)
    vid_vec <- unique(cur_trail_hourly_vv_df$vid)
    vid_cnt <- length(vid_vec)
    cur_training_xreg <- NULL
    if (vid_cnt >= 3) {
      # min_len <- Inf
      # for (episode in episodes) {
      #   cur_vid_hourly_vv_df <-
      #     cur_trail_hourly_vv_df[which(cur_trail_hourly_vv_df$serialno == episode),]
      #   if (length(cur_vid_hourly_vv_df$hour_since_release) < min_len) {
      #     min_len <- length(cur_vid_hourly_vv_df$hour_since_release)
      #   }
      # }
      cur_vv_idx_rng <- c(Inf, -Inf)
      for (vid in vid_vec) {
        cur_vid_hourly_vv_df <-
          cur_trail_hourly_vv_df[which(cur_trail_hourly_vv_df$vid == vid), ]
        cur_vid_idx_rng <-
          range(cur_vid_hourly_vv_df$hour_since_release)
        if (cur_vid_idx_rng[1] < cur_vv_idx_rng[1]) {
          cur_vv_idx_rng[1] <- cur_vid_idx_rng[1]
        }
        if (cur_vid_idx_rng[2] > cur_vv_idx_rng[2]) {
          cur_vv_idx_rng[2] <- cur_vid_idx_rng[2]
        }
      }
      cur_x_idx <-
        seq(from = cur_vv_idx_rng[1], to = cur_vv_idx_rng[2])
      trailer_xreg <- NULL
      for (vid in vid_vec) {
        cur_vv_vec <- rep(NA, length(cur_x_idx))
        cur_vid_hourly_vv_df <-
          cur_trail_hourly_vv_df[which(cur_trail_hourly_vv_df$vid == vid),]
        stopifnot(length(unique(
          cur_vid_hourly_vv_df$vid_plot_name
        )) == 1)
        cur_vid_name <-
          unique(cur_vid_hourly_vv_df$vid_name)
        
        # cur_episode_train_info_df <-
        #   cur_vid_hourly_vv_df[1:min_len, ]
        # cur_trailer_xreg <- cur_episode_train_info_df$vv
        
        cur_vv_vec[cur_vid_hourly_vv_df$hour_since_release] <-
          cur_vid_hourly_vv_df$vv
        cur_trailer_xreg <- cur_vv_vec
        # names(cur_trailer_xreg) <-
        #   cur_episode_train_info_df$hour_since_release
        trailer_xreg <-
          cbind(trailer_xreg, cur_trailer_xreg) # combine vector of DIFFERENT length
        colnames(trailer_xreg)[ncol(trailer_xreg)] <-
          cur_vid_name
      }
      # hour_since_release calibration
      # xreg_all_na_rows <-
      #   trailer_xreg[rowSums(is.na(trailer_xreg)) == ncol(trailer_xreg), , drop = FALSE]
      xreg_all_na_row_idx <-
        which(rowSums(is.na(trailer_xreg)) == ncol(trailer_xreg))
      xreg_all_na_row_idx_rng <- range(xreg_all_na_row_idx)
      all_na_at_head <- xreg_all_na_row_idx_rng[[1]] == 1
      if (all_na_at_head) {
        xreg_all_na_head_row_idx <-
          which(cumsum(c(1, diff(
            xreg_all_na_row_idx
          ) != 1)) == 1)  # https://stackoverflow.com/questions/23095415/how-to-find-if-the-numbers-are-continuous-in-r#23095527
        xreg_all_na_head_row_idx_rng <-
          range(xreg_all_na_head_row_idx)
        stopifnot(
          length(xreg_all_na_head_row_idx) == (
            xreg_all_na_head_row_idx_rng[[2]] - xreg_all_na_head_row_idx_rng[[1]] + 1
          )
        )
        if (length(xreg_all_na_head_row_idx)) {
          # xreg_all_na_head_row_idx <-
          #   as.integer(dimnames(xreg_all_na_rows)[[1]])  # not all trailer_xreg has dimnames[[1]]
          # xreg_all_na_head_row_max_idx <-
          #   xreg_all_na_head_row_idx[length(xreg_all_na_head_row_idx)]
          # xreg_orig_row_idx <-
          #   as.integer(dimnames(trailer_xreg)[[1]])
          xreg_all_na_head_row_max_idx <-
            xreg_all_na_head_row_idx[length(xreg_all_na_head_row_idx)]
          has_row_names <- FALSE
          if (!is.null(dimnames(trailer_xreg)[[1]])) {
            has_row_names <- TRUE
          }
          if (has_row_names) {
            xreg_orig_row_idx <-
              as.integer(dimnames(trailer_xreg)[[1]])
          }
          trailer_xreg <-
            trailer_xreg[which(rowSums(is.na(trailer_xreg)) != ncol(trailer_xreg)), , drop = FALSE]
          
          if (has_row_names) {
            if (length(xreg_all_na_head_row_idx) &&
                length(cur_x_idx) != nrow(trailer_xreg)) {
              cur_x_idx <- cur_x_idx[-xreg_all_na_head_row_idx]
            }
            dim_names <- dimnames(trailer_xreg)
            dimnames(trailer_xreg) <-
              list(cur_x_idx, dim_names[[2]]) # add original row index
          }
          
          cur_trail_hourly_vv_df$hour_since_release <-
            cur_trail_hourly_vv_df$hour_since_release - xreg_all_na_head_row_max_idx
          # stopifnot(length(which(
          #   cur_trail_hourly_vv_df$hour_since_release > 0
          # )) > 0)
          cur_trail_hourly_vv_df <-
            subset(cur_trail_hourly_vv_df, hour_since_release > 0)
        }
        
      }
    } else{
      warn(
        logger,
        sprintf(
          "%s has only %d trailers, skipped.",
          cur_out_comm_prefix,
          vid_cnt
        )
      )
    }
    
    # remove column with less than 3 non-na values(just released video)
    xreg_non_na_colSums <- colSums(!is.na(trailer_xreg))
    if (any(xreg_non_na_colSums < 3)) {
      warn(
        logger,
        sprintf(
          "%s %s is dropped due to too few data.",
          cur_out_comm_prefix,
          colnames(trailer_xreg[, xreg_non_na_colSums < 3, drop = FALSE])
        )
      )
    }
    trailer_xreg <- trailer_xreg[, xreg_non_na_colSums >= 3]
    
    
    if (nrow(cur_trail_hourly_vv_df)) {
      require("scales")
      cur_trail_hourly_vv_plot_df <-
        subset(cur_trail_hourly_vv_df,
               hour_since_release <= max_day_time_span * 24)
      x_rng <-
        range(cur_trail_hourly_vv_plot_df$hour_since_release)
      x_breaks <- seq(x_rng[1], x_rng[2], by = 4)
      p <-
        ggplot(
          data = cur_trail_hourly_vv_plot_df,
          aes(
            x = hour_since_release,
            y = vv,
            color = vid_plot_name,
            group = vid_plot_name
          )
        ) +
        geom_point() +
        geom_line(aes(linetype = vid_plot_name)) +
        scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
        theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
        theme(legend.position = "bottom") +
        xlab("上线小时数") +
        ylab("每小时VV") +
        ggtitle(sprintf("%s 预告片 Hourly VV", cur_out_comm_prefix))
      # print(p)
      ggsave(
        sprintf(
          "%s/%s_%s_trailers_hourly_vv.png",
          png_file_dir,
          cur_bid_name,
          cur_pid_name
        ),
        width = 12,
        height = 6
      )
      
      # generating annotation text
      cur_plot_vid_vec <- unique(cur_trail_hourly_vv_plot_df$vid)
      ann_x <-
        range(cur_trail_hourly_vv_plot_df$hour_since_release)[2] * 0.85
      ann_y <- range(cur_trail_hourly_vv_plot_df$vv)[2] * 0.9
      cur_plot_vid_lab <-
        vapply(cur_plot_vid_vec, function(vid) {
          unique(cur_trail_hourly_vv_plot_df[which(cur_trail_hourly_vv_plot_df$vid == vid), "vid_plot_name"])
        }, FUN.VALUE = "")
      ann_text <- data.frame(
        vid = cur_plot_vid_vec,
        x = ann_x,
        y = ann_y,
        # tentative
        lab = cur_plot_vid_lab
      )
      pat <-
        "([^:：]*)\\s*.*$" # remove long sub title for plotting
      cur_trail_hourly_vv_plot_df$facet_lab <-
        gsub(pat, "\\1", cur_trail_hourly_vv_plot_df$vid_plot_name)
      p <-
        ggplot(data = cur_trail_hourly_vv_plot_df,
               aes(x = hour_since_release, y = vv, color = vid_plot_name)) +
        facet_grid(facet_lab ~ .) +
        geom_point() +
        geom_line(aes(linetype = vid_plot_name)) +
        scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
        theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
        theme(legend.position = "bottom") +
        xlab("上线小时数") +
        ylab("每小时VV") +
        ggtitle(sprintf("%s 预告片 Hourly VV", cur_out_comm_prefix))
      # p <- p +
      #   geom_text(data = ann_text, aes(x = x, y = y, label = lab))
      ggsave(
        sprintf(
          "%s/%s_%s_trailers_facet_hourly_vv.png",
          png_file_dir,
          cur_bid_name,
          cur_pid_name
        ),
        width = 12,
        height = 6
      )
      # vid_daily_vv_sql = paste0(c(
      #   sprintf(
      #     "select date, vid, sum(vv) as vv from vv_vid_day where bid in (%s) and ",
      #     cur_bid_str
      #   ),
      #   sprintf(
      #     "date between %s and %s and vid != -1 and ",
      #     cur_start_date_str,
      #     cur_end_date_str
      #   ),
      #   sprintf(
      #     "vid in (%s) group by date, vid order by date, vv desc",
      #     cur_vid_sql_cond_str
      #   )
      # ))
    }
    
    cur_pid_hourly_vv_df <-
      rbind(cur_hourly_vv_df, cur_trail_hourly_vv_df)
    if (nrow(cur_pid_hourly_vv_df)) {
      cur_pid_hourly_vv_plot_df <-
        subset(cur_pid_hourly_vv_df,
               hour_since_release <= max_day_time_span * 24)
      x_rng <- range(cur_pid_hourly_vv_plot_df$hour_since_release)
      x_breaks <- seq(x_rng[1], x_rng[2], by = 4)
      ggplot(
        data = cur_pid_hourly_vv_plot_df,
        aes(
          x = hour_since_release,
          y = vv,
          group = vid_plot_name,
          color = vid_plot_name
        )
      ) +
        facet_free(serialno ~ .) +
        geom_point() +
        geom_line(aes(linetype = vid_plot_name)) +
        scale_x_continuous(breaks = x_breaks, expand = c(0, 0)) +
        theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
        theme(legend.position = "bottom") +
        xlab("上线小时数") +
        ylab("每小时VV") +
        ggtitle(sprintf("%s Hourly VV", cur_out_comm_prefix))
      ggsave(
        sprintf(
          "%s/%s_%s_vid+trailers_facet_hourly_vv.png",
          png_file_dir,
          cur_bid_name,
          cur_pid_name
        ),
        width = 12,
        height = 6
      )
      
      
      hourly_vv_df_list[[cur_bid_str]][[cur_pid_str]] <-
        list(
          hourly_vv_info_df = cur_hourly_vv_df,
          xreg = xreg,
          trailer_hourly_vv_info_df = cur_trail_hourly_vv_df,
          trailer_xreg = trailer_xreg
        )
    }
    
    
    tryCatch({
      postgresql_dbDisconnectAll(daily.vv.pg.drv)  # timeout or have pending rows
    }, error = function (e) {
      error(logger, as.character(e))
    })
    tryCatch({
      postgresql_dbDisconnectAll(hourly.vv.pg.drv)  # timeout or have pending rows
    }, error = function (e) {
      error(logger, as.character(e))
    })
  } # foer every clipId/pid
  mysql_dbDisconnectAll(mysql.drv)
} # FOR every watching terminal

# get the mean error metrics
sapply(mape_list, function(x) {
  mape_vec <- unlist(x)
  list(
    mean = mean(mape_vec, na.rm = TRUE),
    max = max(mape_vec, na.rm = TRUE),
    max.name = pid_name_list[names(mape_vec)[which.max(mape_vec)]],
    min = min(mape_vec, na.rm = TRUE),
    min.name = pid_name_list[names(mape_vec)[which.min(mape_vec)]],
    median = median(mape_vec, na.rm = TRUE)
  )
})
sapply(rmse_list, function(x) {
  rmse_vec <- unlist(x)
  list(
    mean(unlist(rmse_vec), na.rm = TRUE),
    max = max(rmse_vec, na.rm = TRUE),
    max.name = pid_name_list[names(rmse_vec)[which.max(rmse_vec)]],
    min = min(rmse_vec, na.rm = TRUE),
    min.name = pid_name_list[names(rmse_vec)[which.min(rmse_vec)]],
    median = median(rmse_vec, na.rm = TRUE)
  )
})

program.end.time <- Sys.time()
program.time.taken <- program.end.time - program.start.time
info(logger, sprintf("Total program running time: %s", format(program.time.taken)))