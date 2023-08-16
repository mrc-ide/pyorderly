orderly2::orderly_parameters(x = NULL)
orderly2::orderly_dependency(
  "data",
  quote(latest(parameter:a == this:x)),
  c(incoming.csv = "data.csv"))
d <- read.csv("incoming.csv")
writeLines(sprintf("%d - %s", d$a, d$b), "result.txt")
