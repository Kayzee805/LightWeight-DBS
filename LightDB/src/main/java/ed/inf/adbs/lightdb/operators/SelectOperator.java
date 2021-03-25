package ed.inf.adbs.lightdb.operators;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.Catalog.Catalog;
import ed.inf.adbs.lightdb.Tuple.Tuple;

/**
 * A SelectOperator which extends Operator.
 * It will return tuples that meet the where clause conditions. 
 * Select operator is carried on one single table. 
 * @author kayze
 *
 */
public class SelectOperator extends Operator{
	
	private Operator child1;	//childOperator. For this class it can only be a scanOperator
	private List<Map<String,String>> expressionsUsed; //a List of where clauses that applies to the current table.
	private List<Map<String,String>> expressionsNotUsed; //a List of where clauses that does NOT apply to the current table.
	private List<String> tableNames=new ArrayList<String>(); //a List of table names, for selectOperator it will only have one element.
	private String op; //name of the operator
	private ArrayList<ArrayList<Integer>> sameTable;  //A list which stores whether a expression is an Integer or a column reference. 
	private List<Integer> tableIndex;  //A list which will contain the offset (size of the table) for selectOperator it will just be 0.
	
	/**
	 * Constructor for the SelectOperator.
	 * @param tableName
	 * @param expressionsWithLocations, a List of where expression, which has been separated into
	 * left,right and op expressions. 
	 */
	public SelectOperator(String tableName, List<Map<String,String>> expressionsWithLocations) {
		super();
		this.op = "selector";
		String[] aliasCheck = tableName.split(" ");  //alias check. 
		try {
			child1 = new ScanOperator(Catalog.filePath,aliasCheck[0]+".csv"); //initialise a select operator.
		}
		catch(IOException e) {
			System.err.println("Could not open file in select op");
			e.printStackTrace();
		}
		
		tableIndex = new ArrayList<Integer>();
		tableIndex.add(0);  //initialises an array and adds 0 as we only refer to one table.

		
		tableNames.add(aliasCheck[aliasCheck.length-1]);  //adding the table name to list of tablenames

		String regex="\\-?\\d+";  //regexpression to check for integers.
		
		sameTable=new ArrayList<ArrayList<Integer>>(); //Initialise sameTable Array, which will store whether left or right expression is an int or columnRef.
		
		expressionsUsed = new ArrayList<Map<String,String>>();
		expressionsNotUsed = new ArrayList<Map<String,String>>();
		String left,right;
		int leftInt,rightInt; 
		
		
		//aliasCheck.length-1 because it can have an alias.
		tableName =aliasCheck[aliasCheck.length-1];
		
		//looping over all where clauses
		for(Map<String,String> expression:expressionsWithLocations) {
			left = expression.get("left");
			right = expression.get("right");
			leftInt=0;rightInt=0;

			//if expression is an integer, so no column reference. leftInt =1
			if(left.matches(regex))leftInt=1;
			if(right.matches(regex))rightInt=1;
			
		
			
			
			int isLeft=1,isRight=1;
			
			//if column reference, check its pointing at the same table
			if(leftInt==0) {
				//is string
				String[] checkLeftTable = left.split("\\.");
				if(!checkLeftTable[0].equals(tableName)) {
					//so same table
					isLeft=0;
				}
			}
			if(rightInt==0) {
				String [] checkRightTable =right.split("\\.");
				if(!checkRightTable[0].equals(tableName)) {
					isRight=0;
				}
			}
			
			
			//so if an expression is pointing to a different table
			//add it to the list of expressions not applicable for this table.
			if(isLeft==0 || isRight==0) {

				expressionsNotUsed.add(expression);
			}
			else {
				//else add it to the expression to be used for this table.
				expressionsUsed.add(expression);
				ArrayList<Integer> addToTable= new ArrayList<Integer>();
				
				//this will be further used to check if expression is column or int
				addToTable.add(leftInt);
				addToTable.add(rightInt);
				sameTable.add(addToTable);
			}
		}
	}
	
	/**
	 * @return getter for expressions not used
	 */
	public List<Map<String,String>> getExpressionsNotUsed(){
		return expressionsNotUsed;
	}
	
	/**
	 * @return getter for list of tableNames
	 */
	public List<String> getTableNames(){
		return tableNames;
	}
	
	/**
	 * Getter for tableName
	 */
	public String getSingleTableName() {
		return tableNames.get(0);
	}
	
	//getter for the operator name
	public String getOp() {
		return op;
	}
	
	/**
	 *Returns a tuple that meets the where clauses.
	 *@return Tuple
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currentTuple = child1.getNextTuple();
		if(currentTuple==null)return null;
		boolean tupleEval = evaluate(currentTuple);
		//System.out.println(tupleEval);
		if(tupleEval)return currentTuple;
		else {
			return getNextTuple();
		}
	}
	
	/**
	 * Resets the child operator. 
	 */
	@Override
	public void reset() {
		child1.reset();
	}

	/**
	 * Dumps all the tuples to the console.
	 */
	@Override 
	public void dump() {
		reset();
		Tuple e= getNextTuple();
		while(e!=null) {
			System.out.println(e.toString());
			e= getNextTuple();
		}
		//Done printing so reset to 0
		reset();
	}
	
	
	/**
	 * Check if a tuple meets the conditions of the where clauses.
	 * @param currentTuple
	 * @return boolean.
	 */
	public boolean evaluate(Tuple currentTuple) {
		boolean succeed=true;
		String left,right,operator;
		int leftInt, rightInt;
		
		for(int i=0;i<expressionsUsed.size();i++) {
			left = expressionsUsed.get(i).get("left");
			right=expressionsUsed.get(i).get("right");
			operator = expressionsUsed.get(i).get("op");
			//now check if its valid
			
			//Checking if its all int or has column references
			//call the sameTable which was initialised in the constructor
			leftInt = sameTable.get(i).get(0);
			rightInt=sameTable.get(i).get(1);
			
			//carry out checks for different operators
			//and for each operator, check if both expressions are ints
			//or column references. or if 1 column reference and one int.
			if(operator.equals("=")) {
				if(leftInt==1 &&rightInt==1) {
					succeed=isEqual(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					//so right has to equal 0
					succeed=isEqual(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					//so leftInt ==0 here
					succeed = isEqual(left,Integer.parseInt(right),currentTuple);
				}
				else {
					//both 0, so both are strings
					succeed= isEqual(left,right,currentTuple);
				}
			}
			else if(operator.equals("!=")) {
				if(leftInt==1 &&rightInt==1) {
					succeed=!isEqual(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					//so right has to equal 0
					succeed=!isEqual(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					//so leftInt ==0 here
					succeed = !isEqual(left,Integer.parseInt(right),currentTuple);
				}
				else {
					//both 0, so both are strings
					succeed= !isEqual(left,right,currentTuple);
				}
			}
			else if(operator.equals(">")) {
				//so left is greater than right
				if(leftInt==1 && rightInt==1) {
					succeed= isGreater(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					succeed=isGreater(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					succeed=isGreater(left,Integer.parseInt(right),currentTuple);
				}
				else {
					succeed=isGreater(left,right,currentTuple);
				}
			}
			else if(operator.equals("<=")) {
				//so left is greater than right
				if(leftInt==1 && rightInt==1) {
					succeed= !isGreater(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					succeed= !isGreater(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					succeed= !isGreater(left,Integer.parseInt(right),currentTuple);
				}
				else {
					succeed= !isGreater(left,right,currentTuple);
				}
			}
			else if(operator.equals(">=")) {
				//so left is greater than right
				if(leftInt==1 && rightInt==1) {
					succeed= isGreaterOrEqual(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					succeed=isGreaterOrEqual(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					succeed=isGreaterOrEqual(left,Integer.parseInt(right),currentTuple);
				}
				else {
					succeed=isGreaterOrEqual(left,right,currentTuple);
				}
			}
			else if(operator.equals("<")) {
				//so left is greater than right
				if(leftInt==1 && rightInt==1) {
					succeed= !isGreaterOrEqual(Integer.parseInt(left),Integer.parseInt(right));
				}
				else if(leftInt==1) {
					succeed= !isGreaterOrEqual(Integer.parseInt(left),right,currentTuple);
				}
				else if(rightInt==1) {
					succeed= !isGreaterOrEqual(left,Integer.parseInt(right),currentTuple);
				}
				else {
					succeed= !isGreaterOrEqual(left,right,currentTuple);
				}
			}
			else {
				System.err.println(operator+" is not a valid op");
				System.exit(0);
			}
			
			//if any where clauses fails then return false.
			if(!succeed)return false;
		}
		return succeed;
	}
	
	
	
	/*
	 *Checks if the left and right expressions matches the operator.
	 *Arguments can be either a column reference or integer.
	 *If column reference, get the tuple index from the Catalog.
	 *Only have =,>=,> check as < can just be !>= and <= can be !>
	 */
	//= check
	public static boolean isEqual(int left, int right) {
		return left==right;
	}
	public static boolean isEqual(int left, String right,Tuple x) {
		//tuple x here given in order to keep it consistent with other methods

		int index = Catalog.getIndex(right);
		return left==x.getIndex(index);
	}
	public static boolean isEqual(String left, int right,Tuple x) {
		int index = Catalog.getIndex(left);
		return right==x.getIndex(index);
	}
	public static boolean isEqual(String left, String right, Tuple x) {
		//so both references
		return x.getIndex(Catalog.getIndex(left))==x.getIndex(Catalog.getIndex(right));
	}
	
	/*
	 * > than check.
	 */
	public static boolean isGreater(int left,int right) {
		return left>right;
	}
	public static boolean isGreater(int left, String right, Tuple x) {
		return left>x.getIndex(Catalog.getIndex(right));
	}
	public static boolean isGreater(String left, int right, Tuple x) {
		return x.getIndex(Catalog.getIndex(left))>right;
	}
	public static boolean isGreater(String left, String right, Tuple x) {
		return x.getIndex(Catalog.getIndex(left))>x.getIndex(Catalog.getIndex(right));
	}
		
	
	
	/*
	 * >= than check.
	 */
	public static boolean isGreaterOrEqual(int left,int right) {
		return left>=right;
	}
	public static boolean isGreaterOrEqual(int left, String right, Tuple x) {
		return left>=x.getIndex(Catalog.getIndex(right));
	}
	public static boolean isGreaterOrEqual(String left, int right, Tuple x) {
		return x.getIndex(Catalog.getIndex(left))>=right;
	}
	public static boolean isGreaterOrEqual(String left, String right, Tuple x) {
		//System.err.println(left+" "+right+"  "+x.toString());
		return x.getIndex(Catalog.getIndex(left))>=x.getIndex(Catalog.getIndex(right));
	}
	
	
	//getter for the table index, an array with just 1 element, of 0, for select operator.
	public List<Integer> getTableIndex(){
		return tableIndex;
	}
}
