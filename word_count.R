#! /usr/bin/env Rscript

library(rmr2)
library(rhdfs)
hdfs.init()

iris.data = read.table("iris.data", sep = ",", header = FALSE)
iris.in = to.dfs(iris.data)

map = function(k,v) {
  keyval(v[5], 1)
}

reduce = function(k,vv) {
  keyval(k, sum(vv))
}

iris.out = mapreduce(
  input = iris.in,
  map = map,
  reduce = reduce
)

from.dfs(iris.out)
