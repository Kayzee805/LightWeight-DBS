package ed.inf.adbs.lightdb.Catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Attributes class, the column names and the order of those coloumns for a table
 * @author kayze
 *
 */
public class Attributes {
	
	
	private Map<String,Integer> attributeIndex; //map to store column names and the index of each column
	
	/**
	 * Constructor for Attributes, will initialise the attributeIndex map.
	 * @param fieldNames  will contain both the table names and column names
	 */
	public Attributes(String[] fieldNames) {
		//fieldNames will be the the entire row
		//e.g. TableName Fieldname1 fieldName2,...
		attributeIndex = new HashMap<String,Integer>();
		
		for(int i=1;i<fieldNames.length;i++) {
			//starting from 1 because the first index is the table Name
			attributeIndex.put(fieldNames[i], i-1);
		}
	}
	
	//getter for AttributeIndex
	public Map<String,Integer> getAttributeIndex(){
		return attributeIndex;
	}
	
	//getter for the size of attributeIndex, size of the columns.
	public int getAttrributeSize() {
		return attributeIndex.size();
	}
}
