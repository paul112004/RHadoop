#! /usr/bin/env Rscript
# The K Means algorithm is the modified version of this in the RHadoop wiki as my practice:
# https://github.com/RevolutionAnalytics/RHadoop/wiki/Comparison-of-high-level-languages-for-mapreduce-k-means
library(rmr2)
library(rhdfs)
hdfs.init()

kmeans = function(points, center.count, iterations){
        point.data = to.dfs(points)
        point.count = dim(points)[1]

        # Initialization: Assign points to the centers randomly
        centers = kmeans.iteration(point.data, point.count, NULL, center.count)
        centers = centers$val
        # Iteration: Implementation of K Means algorithm
        for(i in 1:iterations){
                newCenters = kmeans.iteration(point.data, point.count, centers, center.count)
                newCenters = newCenters$val


                # If the new centers are not changed, it means being stationary.
                # We can skip the following loops.
                if(isTRUE(all.equal(centers, newCenters))){
                        break
                }
                centers = newCenters
        }
        # Clustering of points given the centers
        # $key == center, $val == point
        kmeans.iteration(point.data, point.count, centers, NULL)
}

kmeans.iteration = function(point.data, point.count, centers = NULL, center.count = NULL){
        from.dfs(mapreduce(input = point.data,
                map = function(k, v){
                        if(is.null(centers)){
                                # Assign a center to the point randomly
                                keyval(sample(1:center.count, point.count, replace = TRUE), v)
                        }
                        else{
                                # Distance metric: Euclidean distance
                                distances = apply(v, 1, function(i){
                                        apply(centers, 1, function(c){
                                                norm(as.matrix(c - i), type = "F")
                                        })
                                })
                                # Assign the nearest center to the point
                                indices = apply(distances, 2, which.min)
                                newCenters = t(apply(as.matrix(indices), 1, function(i){ t(centers[i, ]) }))
                                keyval(newCenters, v)
                        }
                },
                reduce = function(k, vv){
                        if(is.null(center.count)){
                                # Output all the near points of the center
                                keyval(k, vv)
                        }
                        else{
                                # Compute the new coordinates of a center: The average coordinates of all near points
                                keyval(k, t(as.matrix(apply(vv, 2, mean))))
                        }
                }
        ))
}

# Load data: Iris for example
iris.data = read.table("iris.data", sep = ",", header = FALSE)
sink("iris.out")
kmeans(iris.data[, 1:4], 3, 5)
sink()

