package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ed.inf.adbs.lightdb.Tuple.Tuple;

/**
 * A DuplicateElimiationOperator which extends to Operator.
 * It will return tuples that are unique.
 * @author kayze
 *
 */
public class DuplicateEliminationOperator extends Operator{

	
	private Operator child1; //child operator
	private List<Tuple> visitedTuples; //tuples that have already been called before
	
	/**
	 * Constructor for DuplicateEliminationOperator
	 * @param child1
	 */
	public DuplicateEliminationOperator(Operator child1) {
		super();
		this.child1=child1;
		
		//initialise vistedTuples Array
		visitedTuples=new ArrayList<Tuple>();
		
		//if child was not sort, then sort the child tuples by the first column.
		if(!child1.getOp().equals("sort")) {
			this.child1 = new SortOperator(child1,null);
		}
		//else it is already sorted
	}
	
	
	/**
	 * Return tuple in a sorted order which has not been returned before.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple e;
		while((e=child1.getNextTuple())!=null) {
			//check if it has been returned before
			if(hasVisited(e)==false) {
				visitedTuples.add(e);
				return e;
			}
		}
		return null;
	}
	
	/*
	 * reset the the visited Tuples an empty list, so nothing has been visited.
	 */
	@Override
	public void reset() {
		child1.reset();
		//resetting the arrayList of already called tuples
		visitedTuples=new ArrayList<Tuple>();
	}
	
	/**
	 * Dump all the unique tuples to the console
	 */
	
	@Override
	public void dump() {
		reset();
		Tuple e;
		while((e = getNextTuple())!=null) {
			System.out.println(e.toString());
		}
		reset();
	}
	
	/**
	 * Check if a tuple has been visited/returned before by getNextTuple()
	 * @param e
	 * @return
	 */
	public boolean hasVisited(Tuple e) {
		/*
		 * We start looking bottom to up, as its more likely as the array is sorted.
		 * 
		 */
		for(int i=visitedTuples.size()-1;i>=0;i--) {
			if(Arrays.equals(visitedTuples.get(i).getValues(),e.getValues())) {
				return true;
			}
		}
		return false;
	}
	
}
