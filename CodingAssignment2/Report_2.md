## Task 2 Report

#### Result Analysis: Insights from the QA_Data clustering result

The result of Task 2 is saved in the output folder, and displayed in the console in a more friendly way.

A typical record of the clustering result is like this 

`((150000,488),167,422.0,488.5329341317365)`

indicating the centroid, size, median, average of the cluster, respectively.

We can display the result in the descending order of average score, total score or  size of the cluster, each of which is a indicator of how many question are there close to the topic of the centroid question and how popular the domain of the centroid is. Here is the list of results whose average score is above 1000.

`Domain: Compute-Science, centroid: (50000,10271), size: 2, score sum: 10271.0, average: 10271.5
Domain: Security, centroid: (250000,3661), size: 2, score sum: 3661.0, average: 3661.5
Domain: Machine-Learning, centroid: (0,2550), size: 30, score sum: 2030.0, average: 2550.766666666667
Domain: Algorithm, centroid: (100000,1946), size: 6, score sum: 1677.0, average: 1946.1666666666667
Domain: Big-Data, centroid: (150000,1791), size: 16, score sum: 1400.0, average: 1791.25
Domain: Computer-Systems, centroid: (350000,1234), size: 17, score sum: 971.0, average: 1234.8235294117646
Domain: Compute-Science, centroid: (50000,1099), size: 44, score sum: 929.0, average: 1099.4318181818182
Domain: Data-Analysis, centroid: (200000,1020), size: 28, score sum: 878.0, average: 1020.7857142857143`

From the results can we see that the centroid with largest average score and largest total score is under the domain "Computer Science". This is not strange as this is the super set of all the domains. Higher score means there are many questions in this cluster that get votes, or there are some high quality questions that gets many votes. No matter what, it would be a hot domain. And we take a look at the results. The domains at the front are all the hot areas that we are familiar with, such as Machine Learning, Data Analysis and Big Data. If sorted by cluster size, the result would be like this:

`Domain: Data-Analysis, centroid: (200000,2), size: 352123, score sum: 2.0, average: 2.8937672347446775
Domain: Compute-Science, centroid: (50000,0), size: 240599, score sum: 1.0, average: 0.9808311755244203
Domain: Computer-Systems, centroid: (380739,4), size: 233585, score sum: 2.0, average: 4.474118629192799
Domain: Security, centroid: (250000,4), size: 181447, score sum: 2.0, average: 4.906137880483006
Domain: Machine-Learning, centroid: (0,1), size: 173257, score sum: 1.0, average: 1.3761060159185488`

If a centroid have a large size, there are many questions that linked to the domain of the centroid. The discussion in the domain is quite heated. From the results can we see that Data-Analysis is quite heated discussed during the period that the raw data is collected. Other domains can be explained in the same way.

The data can be explained in more dimensions if the result is organized in other ways, such as including the max score, aggregating the same domain, etc. In that way, we could have a clearer view while comparing  among the domains and activeness of different domains. 

#### Parameter Analysis: K-means Parameters

There are several parameters needed in the kmeans algorithms: domain spread, kernels, estimation and max iteration. 

- Domain spread: determines the extent that different domains disperse from each other, ie, how far between two adjacent domain in the 2-D space. If the spread is large, then clusters of questions with different domain will be more distant from each other as the x value of the vector is defined as spread * domain index. The value should be reasonable if it is large enough compared to the score difference of any two points. Otherwise, questions from 2 different domains could be allocated to a same cluster if there scores do not differ much. If the value is too large, it will still not give any improvement to the accuracy of the algorithm.
- Kernels: the number of initial clustering. The algorithm will generate kernal number of random centroids and take as the initial centroids. It should be approximated to set to be a bit larger than the actual clustering and should be assured not be smaller than the actual clustering number. This is because two cluster would be possible to merge to one cluster with the duplicate new centroids eliminated, but a single cluster cannot be split into more clusters in the processing. So we have to make sure the kernel numebr is a reasonable numebr that a bit larger than the actual number of clusters approximated.
- Estimation: the threshold value for terminating. It is the distance from the previous centroids to the new centroids. If the distance is smaller than the estimation, the optimization process will end. If the value is too large, the algorithm may end before accurately clustering, lacking preciseness. But if the value is too small, it may take much more time to reach the condition as a small fluctuation may cause more iterations, incurring more processing time. So the value should be small to get the accurate results, but not too small.
- Max iteration: this is the number of iterations that the algorithm must terminate, even if the threshold is not met. If the number is too small, the result may not be accurate enough, such that useless. If the number is too large, under the condition that threshold is too small, the algorithm may take extra iterations, even if clustering is done. So we should choose a value that is not too large, but larger than the necessary number of iterations.

#### Discussion on System Performance

To make the algorithm efficient, we can

1. Prune the kmeans parameters as mentioned above, reducing the number of extra meaningless iterations
2. Choose a wise strategy to clean and pre-process the data. Reduce the number of mapping and reducing and other redundant operations. Arrange properly, such as joining after filtering, to reduce the join cost. It is similar to the query optimization process in DBMS.

To speed up the processing, we can:

1. cache the data between two processing procedure
2. Deploy the programme on a cluster to process simultaneously 
3. consider use stream processsing
4. configure a large memory to avoid Spark IO

