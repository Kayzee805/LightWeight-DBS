package ed.inf.adbs.lightdb.Catalog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;

/**
 * Catlog class which will store the information on table names and the table columns.
 * @author kayze
 *
 */
public class Catalog {

	
	public static String filePath; //file path of the schema
	//map of table names and attributes. Attributes contains column names and index of that column in tuple
	public static Map<String, Attributes> tablePath = new HashMap<String,Attributes>();  
	
	
	/**
	 * Will open up the schema file and stores the table names and column names in a map: tablePaths
	 * @param schemaFilePath file path to the schmea directory
	 */
	public static void initialiseCatalog(String schemaFilePath) {
		
		//read the schema file and put table names and attributes into map
		try {
			BufferedReader br = new BufferedReader(new FileReader(schemaFilePath+File.separator+"schema.txt"));
			String line;
			while((line=br.readLine())!=null) {
				String[] row = line.split(" ");
				Attributes temporaryAttributes = new Attributes(row);
				tablePath.put(row[0],temporaryAttributes);
			}
			br.close();
		}
		catch(Exception e) {
			System.err.println("Error in opening schema file path");
			System.exit(0);
		}
		filePath = schemaFilePath+File.separator+"data"+File.separator;
	}
	
	
	/***
	 * If alias exists, put it into the schema and point it to the same attributes as the original table.
	 * @param firstFrom
	 * @param externalJoins
	 */
	public static void addToSchema(FromItem firstFrom,List<Join>externalJoins) {
		
		String[] initailFrom = firstFrom.toString().split(" ");
		tablePath.put(initailFrom[1], tablePath.get(initailFrom[0]));
		
		//do the same if external joins exists
		if(externalJoins!=null) {
			for(Join x:externalJoins) {
				String[] temp = x.toString().split(" ");
				//System.err.println(Arrays.toString(temp));
				tablePath.put(temp[1], tablePath.get(temp[0]));
			}
		}
	
		
	}
	
	/**
	 * Get the index of a column reference. 
	 * e.g. for Sailors.A, it will return the index of column A in the table Sailors
	 * @param x
	 * @return
	 */
	public static int getIndex(String x) {
		String[] splits = x.split("\\.");
		return tablePath.get(splits[0]).getAttributeIndex().get(splits[1]);
	}

	
}
