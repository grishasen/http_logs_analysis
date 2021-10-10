library(arrow)
library(stringr)
library(tidyverse)
library(lubridate)

filepath <- "aws_access_logs.csv"
url_prefix <- "https://www:443/"
individual_url_postfix <- 
  "Container"


queries <- read_csv_arrow(file = filepath)
queries <- queries %>%
  mutate(time_total =
           request_processing_time + target_processing_time + response_processing_time) %>%
  filter(startsWith(request_url, url_prefix)) %>%
  mutate(url = sub(url_prefix, "", request_url))


queries_by_url <- group_by(queries, url)

query_url_stats <- queries_by_url %>%
  summarise(
    cnt = n(),
    min = min(time_total),
    max = max(time_total),
    mean = mean(time_total),
    median = median(time_total),
    P75 = quantile(time_total, probs = 0.75),
    P90 = quantile(time_total, probs = 0.90),
    P95 = quantile(time_total, probs = 0.95),
    P99 = quantile(time_total, probs = 0.99),
    P999 = quantile(time_total, probs = 0.999)
  ) %>%
  arrange(desc(P95))

print(query_url_stats)

container_data <-
  filter(queries_by_url, endsWith(url, individual_url_postfix))
container_data <- mutate(
  container_data,
  time_second = floor_date(
    time,
    unit = "seconds",
    week_start = getOption("lubridate.week.start", 7)
  ),
  time_minute = floor_date(
    time,
    unit = "minutes",
    week_start = getOption("lubridate.week.start", 7)
  )
)

container_stats_per_sec <- container_data %>%
  group_by(time_second) %>%
  summarise(cnt = n()) %>%
  arrange(time_second)

hist_counts_sec <-
  ggplot(data = container_stats_per_sec, aes(x = time_second, y = cnt))
hist_counts_sec + geom_line(color = "blue")

container_stats_per_minute <- container_data %>%
  group_by(time_minute) %>%
  summarise(
    cnt = n(),
    mean = mean(time_total),
    median = median(time_total),
    P90 = quantile(time_total, probs = 0.90),
    P95 = quantile(time_total, probs = 0.95),
    P99 = quantile(time_total, probs = 0.99)
  ) %>%
  arrange(time_minute) %>%
  pivot_longer(
    cols = c(P99, P95
             , mean),
    names_to = "Percentile",
    values_to = "Time"
  )

hist_time_min <-
  ggplot(data = container_stats_per_minute, aes(x = time_minute, y = Time, group = Percentile))
hist_time_min + geom_line(aes(color = Percentile)) +
  scale_color_manual(values = c("green", "blue", "red")) +
  scale_y_continuous(breaks = seq(0, 0.5, 0.05), limits = c(0, 0.5))
