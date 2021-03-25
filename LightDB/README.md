# Advanced Database Systems Coursework

## Extracting where clauses

The entire where clause is provided to a select operator but it will only carry out the clauses which are relevant to it. The SELECT operator is given a list of a map, which contains the the left,op and right expression for each individual where clause. The expression is free of any AND expressions as this has been extracted by the  MyExpresssionDeParser. So, the SELECT operator will only carry out where clauses which contains column references to its relevant table, or has no column references at all. Any unused expression, which is irrelevant for the current table, is then passed on to the next operator in the tree of operators.

## Extracting join clauses 

We carry out the SELECT operator for each of the table in the join clauses, to minimise the amount of tuples that we have to join for. A join operator will take in two child operators, and will carry out the join clauses if a where clause contains column references to both the tables of right and left. To minimise the amount of where clauses, for each of the join operators we take the intersection of the unused where clauses in both the Childs. The reasoning for this is that the unused where clause will only contain where clauses that has not been applied to its child operators and we only focus on the where clauses that has not been used in both the child operators. More on this is explain in the constructor of the join operator.  We create out tree of operators by first creating a join if a join exists (call it **J1**), and for any additional joins, we join it with **J1**, then point this new join to **J1**. This is how we create left deep join. More on this is can be found in the Interpreter class.	

## Project

The project class will take in a list of selected columns, which will be the size and the ordering of the new tuple that we return. We use the Catalog class to get the index of each selected column of our tuple. 

## Sorting 

The sort operator takes in a list which will contain the sorting order. The sort operator is a blocking operator, which means it will have to scan through all the tuples to sort it. We sort it using Java's Collection class and a custom Comparator, this can be found in the constructor of SortOperator. The SortOperator also checks if the ProjectOperator has been called on its child, then change the sortByIndex according to it. More on this can be found in SortOperator class.

## Distinct 

The distinct operator makes sure that no same tuple is written to a file, this is done by first calling a sort on its child operator (sorted by first column if no sort exists), then storing each tuple that has been written/called to a List of *already Visited* tuples. Then each time we call getNextTuple() it will only return the tuple, if it is not in the *already Visited* list of tuples. 

## Query Plan: Interpreter class

The query is executed by the Interpreter class which takes into account of different conditions of the query. It initialises one root Operator then builds that root operator according to the conditions, such as does the query have a where clause, any joins, distinct... More information and details can be found in the Interpreter class. 

 

## 