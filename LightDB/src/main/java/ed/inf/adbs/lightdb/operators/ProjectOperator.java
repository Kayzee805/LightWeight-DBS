package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.lightdb.Catalog.Catalog;
import ed.inf.adbs.lightdb.Tuple.Tuple;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * A projectOperator which extends to Operator.
 * 
 * @author kayze
 *
 */
public class ProjectOperator extends Operator {
	
	private Operator child; //child operator
	private List<SelectItem>selectColumns; //list of columns to be projected
	private List<Integer> tableIndex; //tableIndexs of tables in the child operator
	private List<String> tableNames; //list of tableNames in the child operator
	private String op; //name of the projector
	private List<String> newTupleColumns=new ArrayList<String>(); //new column indices of the new Tuple.

	/**
	 * Constructor for ProjectOperator
	 * @param child, Child operator
	 * @param selectColumns columns to be projected
	 */
	public ProjectOperator(Operator child, List<SelectItem> selectColumns) {
		super(); 	
		this.child =child;
		this.op="project";
		this.selectColumns=selectColumns;
		
		//Storing the select columns to a list
		for(SelectItem x:selectColumns) {
			newTupleColumns.add(x.toString());
		}
		
		//checking if child was join, as it will have multiple tables
		//this way we need to be wary of the offset in the index of tuple.
		if(child.getOp().equals("join")) {
			tableIndex=child.getTableIndex();
			tableNames = child.getTableNames();
		}
		else {
			//if not join, then it only points to one table
			tableNames = child.getTableNames();
			tableIndex = new ArrayList<Integer>();
			tableIndex.add(0);
			if(tableNames.size()>1) {
				System.err.println("Table name size should be 1 in project");
				System.exit(0);
			}
		}
	}
	
	
	/**
	 * It will return a new tuple, which will contain only requested column names.
	 * If join, 
	 * @return Tuple
	 */
	@Override
	public Tuple getNextTuple() {
		
		Tuple currentTuple = child.getNextTuple(); //call tuple from child op
		if(currentTuple==null) {
			return null;
		}
	
		int[] newTupleArray = new int[selectColumns.size()]; //make a new array size of selected columns
	
		//will initialise the array and add values to it.
		for(int i=0;i<newTupleArray.length;i++) {
			String column = selectColumns.get(i).toString();
			int offSet=tableIndex.get(tableNames.indexOf(column.split("\\.")[0]));
			//will get index that the select column is pointing to in the current tuple.
			newTupleArray[i] = currentTuple.getIndex(Catalog.getIndex(selectColumns.get(i).toString())+offSet);
		}
		//return this new array as a tuple
		return new Tuple(newTupleArray);
	}
	
	//getter for the list of columns
	public List<SelectItem> getSelectColumns(){
		return selectColumns;
	}
	
	//dump all the tuples to console then reset it afterwards.
	@Override
	public void dump() {
		reset();
		Tuple e = getNextTuple();
		while(e!=null) {
			System.out.println(e.toString());
			e=getNextTuple();
		}
		reset();
	}
	
	
	//reset the child operator.
	@Override
	public void reset() {
		child.reset();
	}
	
	//getter for the list of table names
	@Override
	public List<String> getTableNames(){
		return tableNames;
	}
	
	//getter for operator name
	@Override
	public String getOp() {
		return op;
	}
	
	//getter for the tableIndexs. 
	public List<Integer> getTableIndex(){
		return tableIndex;
	}
	
	//getter for the columns of the new tuple
	public List<String> getNewTupleColumns(){
		return newTupleColumns;
	}

}
