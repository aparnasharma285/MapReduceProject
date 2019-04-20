#Final Project for CS6240


This is the final project for CS6240

Team members:
1) Ishan Patel
2) Rahul Motan
3) Aparna Sharma


Data Used : We are using the YouTube data from http://socialcomputing.asu.edu/datasets/YouTube2

Aim : We are given 2 files users.csv and group-edges.csv. We will find out the suggested groups for every user using his relation with other users and their relation with a group.
      To find the suggestions, we will first get the list of friends of a user and then get the list of groups every friend is enrolled in. These list of groups will be then used as suggestions by removing all the groups in which the user is already enrolled and which have been already suggested.

Note : Since the dataset we are using is too small, a special Map reduce job has been written which will extend the data with new userIds and groups.
