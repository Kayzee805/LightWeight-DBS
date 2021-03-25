 package ed.inf.adbs.lightdb.operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.lightdb.Tuple.Tuple;

/**
 * A ScanOperator which extends to the class Operator.
 * Will mainly be used to scan rows from a database.
 * @author kayze
 *
 */
public class ScanOperator extends Operator {

	/**
	 * 
	 */
	private BufferedReader br; //A bufferedReader which will scan lines from the database
	private String filePath; //filePath of the database
	private String tableName; //tableName of the database
	public String op; //Operator name
	
	
	/**
	 * Constructor for the ScanOperator.
	 * Will attempt to open the file, if failed it will return an error.
	 * @param filePath
	 * @param tableName
	 * @throws IOException
	 */
	public ScanOperator(String filePath,String tableName) throws IOException {
		super();
		br = new BufferedReader(new FileReader(filePath+tableName));
		this.filePath=filePath+tableName;
		this.tableName = tableName;
		this.op="scanner";
	}
	

	
	/**
	 * @return Tuple the next non null or empty Tuple in the database
	 */
	@Override
	public Tuple getNextTuple() {
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(line!=null && !line.isEmpty()) {
			Tuple tempTuple = new Tuple(line);
			index++;
			return tempTuple;
		}
		else {
			return null;
		}
	}

	
	
	/**
	 * Resets the BufferedReader.
	 */
	@Override
	public void reset() {
		try {
			br = new BufferedReader(new FileReader(filePath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		
	}
	
	/**
	 * Dumps all the tuples into the console.
	 * then resets the the buffered reader.
	 */
	@Override 
	public void dump() {
		reset();
		Tuple e= getNextTuple();
		int counter=0;
		while(e!=null) {
			System.out.println(e.toString()+" Index="+counter++);
			e= getNextTuple();
		}
		
		//Done printing so reset to 0
		reset();
	}
	
	//returns the table name
	@Override
	public String getTableName() {
		return tableName;
	}
	
	//returns the operator name
	@Override
	public String getOp() {
		return op;
	}
	

}
