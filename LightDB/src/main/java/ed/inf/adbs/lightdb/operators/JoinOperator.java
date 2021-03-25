package ed.inf.adbs.lightdb.operators;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.Catalog.Attributes;
import ed.inf.adbs.lightdb.Catalog.Catalog;
import ed.inf.adbs.lightdb.Tuple.Tuple;

/**
 * A JoinOperator which extends to Operator.
 * @author kayze
 *
 */
public class JoinOperator extends Operator {

	private Operator child1,child2;  //join operator will have two child ops.
	private Tuple leftTuple=null,rightTuple=null;
	private List<Map<String,String>> expressionsUsed; //list of where clauses used
	private List<Map<String,String>> expressionsNotUsed; //list of where clauses not used in this operator
	private String op; //operator name
	private List<String> tableNames; //tableNames in both the child operators
	private List<Integer> tableIndex; //tableIndex in both the child operators. 
	
	
	/**
	 * Constructor for the JoinOperator.
	 * @param child1
	 * @param child2
	 */
	public JoinOperator(Operator child1, Operator child2) {
		super();
		this.op = "join";
        this.child1=child1;
		this.child2=child2;
		
		List<Map<String,String>> child1NotUsed= child1.getExpressionsNotUsed();
		List<Map<String,String>>child2NotUsed = child2.getExpressionsNotUsed();
		
		expressionsUsed= new ArrayList<Map<String,String>>();
		expressionsNotUsed = new ArrayList<Map<String,String>>();
		tableIndex = new ArrayList<Integer>();
		
		//tables referenced in the two child operators.
		tableNames = new ArrayList<String>();
		tableNames.addAll(child1.getTableNames());
		tableNames.addAll(child2.getTableNames());
		
		//initialises table Index for all tables.
		initialiseTableIndex();
		

		
		//we only look at expressions that refers to the table in child1 op and child2 op
		//anything else will be for another possible join.
		//essentially we only add the intersection of the expressions not used. 
		for(Map<String,String> expression:child2NotUsed) {
			//adding the intersections
			
			//if expression that is not used in both the childs we look into it
			if(child1NotUsed.contains(expression)) {
			
				String leftTable = expression.get("left").split("\\.")[0];
				String rightTable = expression.get("right").split("\\.")[0];
				
				if(tableNames.contains(leftTable)&& tableNames.contains(rightTable)) {
					//so if left and right are being compared
					expressionsUsed.add(expression);
				}
				else {
					//expression does not reference any table in this operator, so not used
					expressionsNotUsed.add(expression);

				}
			}
		}
		
		
		
	}
	
	
	/*
	 * Initialise the array tableIndex
	 */
	public void initialiseTableIndex() {
		int counter=0;
		//table index will be the offset from different tables.
		//As a we can have a JoinOperator as our child, we check what index of the table joining will be in the tuple
		//e.g. child1=[A,B] child2=[C] so our new tuple will be [A,B,C] but we consider the offset of A,B in the tuple.
		//for [A,B] if we refer to a column in B, we have to add the size of the A and the index of the column in B. 
		for(int i=0;i<tableNames.size();i++) {
			Attributes dummy = Catalog.tablePath.get(tableNames.get(i));
			if(i==0) {
				tableIndex.add(0);
				counter = dummy.getAttrributeSize();
			}
			else {
				tableIndex.add(counter);
				counter+=dummy.getAttrributeSize();
			}
		}
	}
	
	//getter for where clauses not used in this operator.
	public List<Map<String,String>> getExpressionsNotUsed(){
		return expressionsNotUsed;
	}
	
	//getter for tableNames
	@Override
	public List<String> getTableNames(){
		return tableNames;
	}
	
	//getter for the operator name
	@Override
	public String getOp() {
		return op;
	}
	
	//getter for the list of tableIndex
	public List<Integer> getTableIndex(){
		return tableIndex;
	}
	
	/*
	 * Returns a tuple that matches the where clauses. The where clauses will only contain
	 * column references on both the side.  Does a left deep join. 
	 */
	@Override
	public Tuple getNextTuple() {
		
		//to pass the initial step. leftTuple and rightTuple initialised at the start.
		if(leftTuple==null && rightTuple==null) {
			
			leftTuple=child1.getNextTuple();
			
			if(leftTuple==null) {
				return null; //because nothing in left child so return null
			}
			rightTuple = child2.getNextTuple();
			if(rightTuple==null) {
				return null; //because nothing in the right child so return null
			}
		}
		else {
			if(leftTuple!=null && rightTuple!=null) {
				//if both are not null, search for the next tuple
				
				rightTuple = child2.getNextTuple();
				if(rightTuple== null) { //if rightTuple is null, we check for next tuple of left
					//go next on the left child
					leftTuple=child1.getNextTuple();
					
					if(leftTuple==null) { //if left tuple is null, then we have finished iterating over all the tuples
						return null; //finished
					}
					//if leftOperator still has tuples, we reset the 2nd child op.
					child2.reset();
					rightTuple = child2.getNextTuple();
				}
			}
		}
		if(leftTuple==null) { //if left tuple is ever null, we have finished
			return null;
		}
		//check if it matches the where clauses
		boolean acceptNewTuple = evaluateJoin(leftTuple,rightTuple);
		
		//if it matches, return a new tuple which is the Concatenation of the two tuples.
		if(acceptNewTuple) {
			return combineTwo(leftTuple,rightTuple);
		}
		
		//if a tuple was not returned earlier, it was not found
		//so search for the next non null tuple.
		return getNextTuple();
	}
	
	/**
	 * Concatenation of two tuples
	 * @param left leftTuple
	 * @param right rightTuple
	 * @return
	 */
	public Tuple combineTwo(Tuple left,Tuple right) {
		int[] leftArray=left.getValues();
		int[] rightArray=right.getValues();
		int[] newArray = new int[leftArray.length+rightArray.length];
		int i=0;
		for(int x:leftArray) {
			newArray[i]=leftArray[i];
			i++;
		}
		int leftSize = leftArray.length;
		for(int x:rightArray) {
			newArray[i]=rightArray[i-leftSize];
			i++;
		}
		return new Tuple(newArray);
	}
	
	/**
	 * 	Check if the tuples meet the where clauses. Similar to selectOperator but this time, both expressions
	 * are column references.
	 * @param leftTuple
	 * @param rightTuple
	 * @return
	 */
	public boolean evaluateJoin(Tuple leftTuple, Tuple rightTuple) {
	
		boolean succeed=true;
		boolean isRight =false;
		String left,right,operator;
		int leftInt,rightInt;
	
		
		for(int i=0;i<expressionsUsed.size();i++) {
			isRight =false;
			left = expressionsUsed.get(i).get("left");
			right = expressionsUsed.get(i).get("right");
			operator = expressionsUsed.get(i).get("op");

			//check which expression is referring to which child. child1 or child2
			if(right.split("\\.")[0].equals(child2.getSingleTableName())) {
				//if equals, rightexpr is column referencing table from 2nd child
				isRight=true;
			}
			
			// right expression refers to right table
			if(isRight) {
				rightInt = rightTuple.getIndex(Catalog.getIndex(right));
				
				//offset is used, in case of a child being a join operator with multiple tables
				//offset is the size of the table. so for tuple with tables [A,B] the offset for A=0 and B= size of A
				int offset = tableIndex.get(tableNames.indexOf(left.split("\\.")[0]));
				leftInt = leftTuple.getIndex(Catalog.getIndex(left)+offset);
			}
			//right expression refers to the left table.
			else {
				leftInt = rightTuple.getIndex(Catalog.getIndex(left));
				int offset = tableIndex.get(tableNames.indexOf(right.split("\\.")[0]));
				rightInt = leftTuple.getIndex(Catalog.getIndex(right)+offset);
				
			}
			
			//now do the operator checks to see if where clause is met.
			if(operator.equals("=")) {
				succeed= SelectOperator.isEqual(leftInt, rightInt);
			}
			else if(operator.equals("!=")) {
				succeed= !SelectOperator.isEqual(leftInt, rightInt);

			}
			else if(operator.equals(">")) {
				succeed= SelectOperator.isGreater(leftInt, rightInt);

			}
			else if(operator.equals("<=")) {
				succeed= !SelectOperator.isGreater(leftInt, rightInt);

			}
			else if(operator.equals(">=")) {
				succeed= SelectOperator.isGreaterOrEqual(leftInt, rightInt);

			}
			else if(operator.equals("<")) {
				succeed= !SelectOperator.isGreaterOrEqual(leftInt, rightInt);

			}
			else {
				System.err.println("Invalid op in join: "+operator);
				System.exit(1);
			}
			
			//if succeed is ever false, then that means where clause is not met.
			if(!succeed)return false;
		}
		
		return succeed;
	}
	
	/*
	 * Resets both the child operators and then sets leftTuple and rightTuple to null.
	 */
	@Override
	public void reset() {
		child1.reset();
		child2.reset();
		leftTuple=null;
		rightTuple=null;
	}
	
	/**
	 * Dumps all the tuples of the join in the console.
	 */
	@Override
	public void dump() {
		reset();
		Tuple e = getNextTuple();
		int counter=0;
		while(leftTuple!=null) {
			System.out.println(e.toString()+" index="+counter++);
			e=getNextTuple();
		}
		reset();
	}
	
}
