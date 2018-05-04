# https://stats.stackexchange.com/questions/169468/how-to-do-forecasting-with-detection-of-outliers-in-r-time-series-analysis-pr#answer-173684

require(tsoutliers)
x <- c(
  7.55,
  7.63,
  7.62,
  7.50,
  7.47,
  7.53,
  7.55,
  7.47,
  7.65,
  7.72,
  7.78,
  7.81,
  7.71,
  7.67,
  7.85,
  7.82,
  7.91,
  7.91,
  8.00,
  7.82,
  7.90,
  7.93,
  7.99,
  7.93,
  8.46,
  8.48,
  9.03,
  9.43,
  11.58,
  12.19,
  12.23,
  11.98,
  12.26,
  12.31,
  12.13,
  11.99,
  11.51,
  11.75,
  11.87,
  11.91,
  11.87,
  11.69,
  11.66,
  11.23,
  11.37,
  11.71,
  11.88,
  11.93,
  11.99,
  11.84,
  12.33,
  12.55,
  12.58,
  12.67,
  12.57,
  12.35,
  12.30,
  12.67,
  12.71,
  12.63,
  12.60,
  12.41,
  12.68,
  12.48,
  12.50,
  12.30,
  12.39,
  12.16,
  12.38,
  12.36,
  12.52,
  12.63
)
x <- ts(x, frequency = 12, start = c(2006, 1))
res <- tso(x, types = c("AO", "LS", "TC"))

# define the variables containing the outliers for
# the observations outside the sample
npred <- 12 # number of periods ahead to forecast
newxreg <- outliers.effects(res$outliers, length(x) + npred)
newxreg <- ts(newxreg[-seq_along(x), ], start = c(2012, 1))

# obtain the forecasts
p <- predict(res$fit, n.ahead = npred, newxreg = newxreg)

# display forecasts
plot(
  cbind(x, p$pred),
  plot.type = "single",
  ylab = "",
  type = "n",
  ylim = c(7, 13)
)
lines(x)
lines(p$pred, type = "l", col = "blue")
lines(p$pred + 1.96 * p$se,
      type = "l",
      col = "red",
      lty = 2)
lines(p$pred - 1.96 * p$se,
      type = "l",
      col = "red",
      lty = 2)
legend(
  "topleft",
  legend = c("observed data",
             "forecasts", "95% confidence bands"),
  lty = c(1, 1, 2, 2),
  col = c("black", "blue", "red", "red"),
  bty = "n"
)