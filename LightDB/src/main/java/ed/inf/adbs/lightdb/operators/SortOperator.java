package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ed.inf.adbs.lightdb.Catalog.Catalog;
import ed.inf.adbs.lightdb.Tuple.Tuple;

/**
 * A SortOperator which extends Operator
 * It will sort all the tuples from its child operator.
 * SortOperator is a blocking operator so will store all tuples in a list.
 * according to the ORDER BY clause. 
 * @author kayze
 *
 */
public class SortOperator extends Operator{
	
	private Operator child1; //Child operator
	private List<Tuple> allTuples=new ArrayList<Tuple>(); //stores the list of tuples 
	private int index=0; //index used as counter on which tuple to return by getNextTuple()
	private String op; //name of the operator
	
	public SortOperator(Operator child1,String[] orderBy) {
		super();
		this.child1=child1;
		this.op = "sort";
		

		List<Integer> tableIndex = child1.getTableIndex();
		List<String> tableNames = child1.getTableNames();
		
		//generate all the tuples, as SortOperator is a blocking operator
		generateAllTuples();
		
		
		List<Integer>sortByIndex = new ArrayList<Integer>();
		int tupleIndex,offset;
		
		//if no SortByIndex then sort by the first column, used by the distinct operator.
		if(orderBy==null) {
			sortByIndex.add(0);
		}
		else {
			//add the sort by indices of the tuple to an arrayList.
			
			//if child operator is project then it has tuples with different indices
			//so need to check for it
			if(child1.getOp().equals("project")) {
				//has new tuple
				List<String>newColumns= child1.getNewTupleColumns();
				for(String x:orderBy) {
					sortByIndex.add(newColumns.indexOf(x));
				}
			}
			else {
			
				for(String x:orderBy) {
					offset = tableIndex.get(tableNames.indexOf(x.split("\\.")[0]));
					tupleIndex = Catalog.getIndex(x);
					sortByIndex.add(offset+tupleIndex);
				}
			}
		}
	
		
		//using Collections and custom comparator to sort all the tuples
		Collections.sort(allTuples, new Comparator<Tuple>() {  
		    @Override  
		    public int compare(Tuple p1, Tuple p2) {  
		     	for(int x:sortByIndex) {
			      	int a=(p1.getIndex((x)));
			    	int b=(p2.getIndex((x)));
			    	
			    	if(a>b) {
			    		return 1;
			    	}
			    	if(a<b)return -1;
		    	}
		    	return 0;
		    }  
		});
		
	}
	
	
	//generate all the tuples and store it to a list
	public void generateAllTuples() {
		Tuple currentTuple;
		while((currentTuple=child1.getNextTuple())!=null) {
			allTuples.add(currentTuple);
		}
	}
	
	/*
	 * Returns a tuple from the list of tuples.
	 * and index is incremented each time
	 * if index is out of bounds, return null
	 */
	@Override
	public Tuple getNextTuple(){
		if(index>=allTuples.size()) {
			return null;
		}
		else {
			Tuple e = allTuples.get(index);
			index++;
			return e;
		}
	}
	
	//set index to 0 so, when getNextTuple() is called again, it calls the first tuple
	@Override
	public void reset() {
		index=0;
	}
	
	/*
	 * dumps all the tuples to the console and then resets it.
	 */
	@Override
	public void dump() {
		reset();
		for(Tuple e:allTuples) {
			System.out.println(e.toString());
		}
		reset();
		
	}
	
	//getter for the operator name
	@Override
	public String getOp() {
		return op;
	}


}
