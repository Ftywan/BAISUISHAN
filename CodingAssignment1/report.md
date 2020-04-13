# CS4225 Assignemnt 1 Report

#### Fu Tianyuan A0177377M

##### Task 1: Word Count

For the task 1, I was following the paradigm shown in the slides, deviding the algorithm into 4 stages. 

Step 1 and step 2 are meant for reading the two input files and coverting to two <word -> count> pairs. During the setup process, we have read the stop words into a HashMap. While doing the mapping for the first two stages, we will check if the lower case of the key value is in the stop word list with O(1) effort. If in, we will disgard it and not pass to the reducer.

Step 3 is used for adding a identifier to the two mappings which are from two different files. As they would be added with different identifiers, we are using two mappers, one for appending "s1" to the values, one for "s2". The reason why we need identifiers for the values is because while counting the common words, we have to ensure the common words are appearing in both files. If the pair is only appearing in one file, we should disgard the word because it does not show in the other file. The reducing process will take the lower count if there are two pairs: one for s1 and one for s2. If the two identifiers do not show at the same time during the reducing process, we are discarding the key. 

Step 4 is mainly meant for sorting. The sorting is not a in-memory sorting done by any algorithm; rather, it is done during the *Shuffle and Sort* process. The engine will collect the mapping result by the ascending order of keys and then pass to reducers. So the mapper of step 4 take the count as the key as we are going to sort by key. The inverse number of the count number is taken as the key as we want to reverse the sorting order. In the reducer, we have to shift the key back to maintain the correct count value. As we are only asking for 15 values, a static counting variable can hold the current counts so no more data will be emitted after 15 counts.

Here is a sample output:

```java
196 a
81 project
54 gutenberg-tm
33 work
27 electronic
22 works
22 gutenberg
21 terms
19 will
15 full
14 donations
14 foundation
14 good
13 archive
13 literary
```



##### Task 2

This task is a derivative to the matrix multiplication problem. The program mostly follows the templates in the slides, with minor changes to the matrix multiplication.

Step 1 is building the history matrix based on the input <id, item, score> pair. As the history and score are on the per-user basis, the mapper takes the id as the key, the (item, score) pair as the value. The reducer will combine all the user-related pairs into one string, emit as the new value. The sample output of step 1 is as the follow:

```java
2   404:4.5,101:1.5,102:2.5,321:1.0,482:0.5
```

Step 2 is for building the co-occurrence matrix based on step 1 output. As the co-occurrence is only meaningful while for one person, we are taking step 1 output as the input. The mapper functions will take the activity records for a single user, and among all the items, take the pair of any two item as the key, and *one* as the value, sending to the reducer to do the counting. A sample mapping would be:

```java
1,101   1
```

The counting process is similar to wordCount. The reducer will keep the same key, and do the counting of records going into the reducer, emitting the counting as the new value to the key. Here is a sample output of co-occurrence matrix:

```java
1,10    15
```

Step 3 is for the matrix cleaning and altering, for the ease of matrix multiplication. During the matrix multiplication, say for a single user, we are calculating the recommendation score for item 1, we are summing up the weighted score (occurrence times * score) of item2(s), which are co-occurring items of item 1. Data needed for this multiplication is <item1, item2 -> occurrence> and <id, item2 -> score>. Step 4 is the entry stage of matrix multiplication, requiring the two input data sets having the same key. Therefore, in step 3, we are converting the two sets to the form of <item2 -> item1, occurrence> and <item2 -> id, score>.

The step 3_1 is mapping the original data to reorganize the sequence to be <item -> id, score> and there is no reducer in this step. The sample output is:

```java
1   680,1.5
```

The step 3_2 is taking the output of step 2, mapping the data to the form of <item -> item, 15>. <item1, item2> should have the same value with <item2, item1> by default. There is no reducer needed for this step as well. Here is a sample output of step3_2:

```java
1   250,10
```

Step 4 is taking the outputs of step 3, carrying out the matrix multiplication. Step 4_1 is emmitting the two versions of outputs of step 3 and reduce them to one single string. Since the values have two types, identifiers as "A" and "B" are added to the front of each appended values. "A" is for (id, score) pair and "B" is for (item, occurrence) pair. A partial output is like:

```java
15  A:859,3.5 A:657,2.0 A:269,3.0 A:226,2.5 A:959,3.5 B:489,17 B:488,9 B:487,12 
```

Step 4_2 is doing the multiplication of any (A, B) pair for a single item 1, and summing up to get the recommendation score. The mapper will first read and parse the values of item 2, pushing the (id, score) pair and (item 1, occurrence) to two seaprate LinkedList, then iterate through the two lists, getting the weighted score by multiply the score by occurrence, for the item 1 of a specific person. The mapper will emit <id, item1 -> score> in the end. The reducer will sum up the score of the item 1 for the user, and emit the mapping <id, item -> score>. A sample output of step 4_2 is ;

```java
737,279 1922.0
```

So far the calculation has been done. The step 5 is for selecting the person with a certtain id and get the result sorted. The mapper will first parse the id, if matching the specified id (737 in my case), then emit a mapping with re-organization in the form <score -> item>. Score is taken as the key for sorting and "-1" is multiplied for reverse order sorting. The reducer does nothing except converting the key back to the positive value and write to the output in the form <score -> item>. The top 10 recommendations in my case is:

```java
3129.5  420
3029.5 454
2889.5 58
2869.5 5
2809.0 182
2736.0 343
2728.5 445
2708.0 245
2683.0 494
2673.5 403
```

Algorithm explanation finished.

