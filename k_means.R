#! /usr/bin/env Rscript
# The K Means algorithm is the simplified version of this in the RHadoop wiki as my practice (NOT TESTED COMPLETELY):
# https://github.com/RevolutionAnalytics/RHadoop/wiki/Comparison-of-high-level-languages-for-mapreduce-k-means
library(rmr2)
library(rhdfs)
hdfs.init()

kmeans = function(points, center.count, iterations){
	point.data = to.dfs(points)
	
	# Initialization: Assign points to the centers randomly
	centers = kmeans.iteration(point.data, center.count, NULL)
	centers = centers$val

	# Iteration: Implementation of K Means algorithm
	for(i in 1:iterations){
		newCenters = kmeans.iteration(point.data, center.count, centers)
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
	kmeans.iteration(point.data, NULL, centers)
}

kmeans.iteration = function(point.data, center.count = NULL, centers = NULL){
	from.dfs(mapreduce(input = point.data,
		map = function(k, v){
			if(is.null(centers)){
				# Assign a center to the point randomly
				keyval(sample(1:center.count, 1), v)
			}
			else{
				# Distance metric: Euclidean distance
				distances = apply(centers, 1, function(c){ norm(as.matrix(c - v), type = "F") })
				# Assign the nearest center to the point
				keyval(centers[which.min(distances), ], v)
			}
		},
		reduce = function(k, vv){
			# mean == function(c){ mean(c) }
			if(is.null(center.count)){
				# Output all the near points of the center
				keyval(k, vv)	
			}
			else{
				# Compute the new coordinates of a center: The average coordinates of all near points
				keyval(NULL, apply(rbind(vv), 2, mean))
			}
		}
	))
}

# Load data: Iris for example
iris.data = read.table("iris.data", sep = ",", header = FALSE)
kmeans(iris.data[, 1:4], 3, 10)
