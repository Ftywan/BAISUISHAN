## Task 1 Report

#### Implementation

The implementation is quite straightforward in spark. For stoplist, we read in the records as a RDD and convert to a Set for later checking. Then we read in both input files, use a flatmap function and regex to remove all the characters that are not a-z (case insensitive), dash(-), and single quotation mark('). Then we filter out all the empty words and words appearing in the stop word list. Then, we do a partial map reduce to count the words appearing in the single file. We inner join the two count and select the smaller value with the same key. Finally, we reverse the key and value, sort by value and output. Done!

#### Comparisons on programming with Hadoop and Spark

Implementing a project with hadoop requires more details in the designing progress. Hadoop executes in a job-by-job fashion and we have to configure the job details on ourselves, such how many mapper tasks and reduce tasks we wannt to use in the job. The job also set some constrains to programs. It can only do a simple map reduce operation because hadoop is transferring data by file. As for the map function and reduce function, we have to do a lot of type conversions, string manipulations and so on in Hadoop, and the only supported functions are java libraries. 

Things turns a lot better in spark. With the help of Scala, we can creat a Spark Context really quickly and convenient. The whole program is out of the restrictions of the number of mapping task and reducing task, and programmed in a more understandable way. Spark introduces bunch of functions to be applied on it own data types like RDD and Data Frame, and methods to convert among each other quickly. It also supports multiple languages to implement, such as python and java. The supported functions are more natural to user and anaylists as they share many common concepts with queries and operations of database, such as join(), filter() and groupby(). It also provides the probabilty to do ML and process using SQL queries. 

However, in my opinion, Hadoop is more natual to programmers id they do have any previous knowledge to data management systems because it is using the language that most of the programmers are familiar with. Also confguring an environment for spark is easy and the dependency hierarchy is much clear. It would be a good choice if the project is much complicated. 

For Spark, there is not always the benefits. As spark is type-ortiented, the convertions and manipulations to data records is not as free as in Hadoop. We have to be more cautious when manipulating in Spark. Also, if implementing in Scala, Scala would be a bit trickier than normal programming languages as it is a functional programming language, which regards every function as a value. But through practices functional programming can be quite useful and fast to implement if the developer is familiar with it. 

In short, Hadoop is more complicated, unfriendly to analyst and dummies, but providing more things to manipulate and customize. Spark is easy to implement, friendly to all users, and there are bunch of libraries to use for a project, but if using Scala language, functional programming may make people feel unnatural.

#### Comparison on runtime execution with Hadoop and Spark

The running time for the implementationof Common Words in spark is around 3 seconds and that in Hadoop is around 5 seconds. The main difference in my opinion is Hadoop is using file to transfer data between jobs, which may incur random IOs and data trasnferring time, while spark may keep the data in memory and ready for the next use. The computer I am using is using PCIE SSD, which to some extent minimizes the disadvanteage of Hadoop. If applied on hardwares using HDD, which is a common case in data centers,  or remote storage, the running time for Hadoop may seem much longer than that of Spark.

So, in most of the cases, the performance of spark should be better than Hadoop as it caches the hot data. But is Hadoop always eclipsed by Spark?

In my opinion, the performance of the two framework depends. Hadoop is using a more primitive way to process and transfer data, by file and via HDD, but it actually lowers down the requirement for the machine that is running Hadoop, which is in the same trend with the "Scaling-out" strategy. Spark is faster but it is using the memory to trade for the performace and memory is much more expensive than harddrive. Therefore, Hadoop would still be a good option for cases that are sensitive to budgets but still want to leverage the map reduce strategy.

In short, as the next generation of the implementation of MapReduce paradigm, spark is trading memory for better performance and easier implementation than Hadoop. It uses caching to pass data and supports lots of useful methods. However, Spark is too heavy on memory and Hadoop can still be useful as it is not heavy on memory and can still provide reasonable performance in productive environment.

