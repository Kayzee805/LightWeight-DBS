# LightWeight-DBS



## Coursework for Advanced Database Systems course

This assignment consisted of using CCJSQL to parse SQL queries, an implementation for common SQL operators (selection, project, join, sort and distinct) and then creating a query plan which will execute the SQL query using the operators. 

## Notice

1. For this project only left deep joins were implemented.
2. All the tables either have an alias or they don't. 
3. Sort is only called after projection, so we don't consider cases of sorting by columns which are not projected. e.g. SELECT S.A From S ORDER BY S.B; will be invalid for this project.