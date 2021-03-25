

package ed.inf.adbs.lightdb.operators;

import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.Tuple.*;

/**
 * An abstract operator class which all the operators will extend to.
 * @author kayze
 *
 */
public abstract class Operator {
	
	/**
	 * Some variables which will be used by most of the child operators
	 */
	protected int index;
	private Operator childOp;
	private String tableName;
	public String op;
	public List<String> tableNamesOfChild;
	
	
	
	public Operator() {};
	
	//can take multiple child ops

	/**
	 * Abstract method for returning the next tuple from a table
	 * @return Tuple 
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Abstract method which resets the order of getNextTuple() back to 0.
	 */
	public abstract void reset();
	
	/**
	 * Abstract method which will call getNextTuple() until it has no more tuples to call. Then resets it. 
	 */
	public abstract void dump();

	/**
	 * Returns the current table name, used by scanner operator.
	 * @return tableName
	 */
	public String getTableName() {return null;};
	
	/**
	 * Return a list of where expressions not used. Mainly used for joins, further detail can be found on ReadMe or JoinOperator.
	 * @return list of a map.
	 */
	public List<Map<String,String>> getExpressionsNotUsed(){
		return null;
	}
	
	/**
	 * Returns a list of table names. Used by joins to check which tables have been accessed.
	 * @return list of table names.
	 */
	public List<String> getTableNames(){
		return null;
	}
	
	/**
	 * Returns the name of the current operator. e.g. scanner, join, select, distinct...
	 * @return opName
	 */
	public String getOp() {
		return null;
	}
	/**
	 * Returns the name of the current table. Used by Select operator.
	 * @return tableName
	 */
	public String getSingleTableName() {
		return null;
	}
	
	/**
	 * @return A list of integer, where integers represent the 
	 */
	public List<Integer> getTableIndex(){
		return null;
	}
	
	/**
	 * @return A list of String, which will be the name of the columns. 
	 */
	public List<String> getNewTupleColumns(){
		return null;
	}
}
