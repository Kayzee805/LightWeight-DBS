package ed.inf.adbs.lightdb;
//just importing the other classes

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;


/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {
	/*
	 * So first need a scanner operator which needs to extend teh operator class
	 * Probably need to use the schema.txt to figure out whats what?
	 * so that way I can figure out whats the field for a database.
	 */

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		Interpreter.execute(databaseDir, inputFile, outputFile);
		
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			//Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats WHERE Boats.D = 107");
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				
				PlainSelect plain = (PlainSelect)select.getSelectBody();
				List<SelectItem> selectitems = plain.getSelectItems();
				System.out.println("Select items: "+Arrays.toString(selectitems.toArray()));
				System.out.println("From: "+plain.getFromItem());
				
				
				//this is what they wanted, how we get teh table name
				FromItem firstFrom = (FromItem) plain.getFromItem();	
				List<Join> joinsFrom = new ArrayList<Join>();
				joinsFrom = plain.getJoins();
				
				System.out.println("From using fromItem: "+firstFrom);
				if(joinsFrom!=null) {
					System.out.println("Join froms :"+Arrays.toString(joinsFrom.toArray()));
				}
				else {
					System.out.println("No joins from");

				}
				
				TablesNamesFinder  tablesNamesFinder = new TablesNamesFinder ();
				List<String> tableList = tablesNamesFinder.getTableList(select);
			
				System.out.println("Table Names: "+Arrays.toString(tableList.toArray()));
				System.out.println("Select body is " + select.getSelectBody());
				
				Expression wheres = plain.getWhere();
				System.out.println(wheres);
				Expression expressions = CCJSqlParserUtil.parseCondExpression(plain.getWhere().toString());
				System.out.println("Testing "+expressions.toString());
				

			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
