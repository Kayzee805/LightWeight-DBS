package ed.inf.adbs.lightdb.Tuple;


/*
 * A Tuple class that stores the values in an array.
 */
public class Tuple {
	
	/**
	 * Field values are stored in an array, insertion order maintained.
	 */
	private int[] fieldVals;
	
	/**
	 * Constructor for Tuple class. Initialises the fieldVals variable.
	 *
	 * @param row a row of a table where each column is separated by a comma
	 */
	public Tuple(String row) {
		//split it by comma and store values into an array
		String[] values = row.split(",");
		
		//Parse the string values to integers and store it in fieldVals.
		fieldVals = new int[values.length];
		for(int i=0;i<values.length;i++) {
			fieldVals[i]=Integer.parseInt(values[i]);
	}
	}
		
	/**
	 * Second constructor which takes in an array of int instead of String.
	 * @param values field value/
	 */
	public Tuple(int[] values) {
		fieldVals = values;
	}
		
			
	/**
	 * Getter for values
	 * @return returns the entire value array
	 */
	public int[] getValues(){
		return fieldVals;
	}
	
	/**
	 * @return string version of the field values of a row.
	 */
	public String toString() {
		String returnString="";
		for(int i=0;i<fieldVals.length;i++) {
			if(i==fieldVals.length-1) {
				returnString+=fieldVals[i];
			}
			else {
				returnString+=fieldVals[i]+",";
			}
		}
		return returnString;
	}
	
	/**
	 * Returns a the value stored at a specific index. So getting the value of a certain column.
	 * @param index  index of the column
	 * @return value of the row at that column
	 */
	public int getIndex(int index) {
		return fieldVals[index];
	}
	
	
}
