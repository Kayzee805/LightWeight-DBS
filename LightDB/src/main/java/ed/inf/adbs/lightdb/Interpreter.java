package ed.inf.adbs.lightdb;
import ed.inf.adbs.lightdb.Catalog.Catalog;
import ed.inf.adbs.lightdb.Tuple.Tuple;
import ed.inf.adbs.lightdb.operators.DuplicateEliminationOperator;
import ed.inf.adbs.lightdb.operators.JoinOperator;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.operators.SortOperator;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import ed.inf.adbs.lightdb.Utils.MyExpressionDeParser;
import ed.inf.adbs.lightdb.Utils.MyExpressionVisitor;

/**
 * Interpreter for the query. Will construct a query plan then write it to a file.
 * @author kayze
 *
 */
public class Interpreter {
	
	/**
	 * Will construct a query plan. 
	 * @param databaseDir directory for the schema
	 * @param inputFile  sql query path
	 * @param outputFile output path
	 */
	public static void execute(String databaseDir,String inputFile,String outputFile) {
		try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
            if(statement!=null) {
            	//System.out.println("Read statement: "+statement);
            	
            	//initialisation of the objects
            	Select select = (Select)statement;
            	PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
            	List<SelectItem> selectItems = plainSelect.getSelectItems();
            	Distinct distinct = plainSelect.getDistinct();
            	String fileSeperator = File.separator;
            	
            	//initialising the Catalog class
            	Catalog.initialiseCatalog("samples"+fileSeperator+"db");
            	
            	//getting the FROM clause and any joins
            	FromItem firstFrom = (FromItem) plainSelect.getFromItem();
            	List<Join> externalJoins = plainSelect.getJoins();
            	//checking if schema exists, if so add it to catalog
            	//alias has to be prefixed, so either all tables have it or no tables have it.
            	if(firstFrom.getAlias()!=null) {
            		Catalog.addToSchema(firstFrom, externalJoins);
            	}
            	
            	//getting the orderBy list, which is the sort order
            	List<OrderByElement> orderByList = plainSelect.getOrderByElements();
            	
            	//checking if some conditions exists. Distinct, select All and orderyBy
            	boolean isDistinct = distinct==null? false:true;
            	boolean printAll = (selectItems.get(0).toString().equals("*"))? true:false;
            	boolean orderByValid = false;
            	String[] orderByArray;
            	
            	//check if orderBy exists, if it does initialise the array which will contain the order of sort.
            	if(orderByList!=null) {
            	orderByArray = new String[orderByList.size()];
            	for(int i=0;i<orderByList.size();i++) {
            		orderByArray[i] =orderByList.get(i).toString();
            	}
            	orderByValid=true;
            	}
            	else {
            		orderByArray=null;
            	}
            	
            	
            	//get all the where clauses for the query
            	Expression whereClause = plainSelect.getWhere();
            	List<String> expressionList = new ArrayList<String>();
            	
            	//if where clause exists, extract it as individual expressions.
            	if(whereClause!=null) {
            		
                	MyExpressionDeParser parser= new MyExpressionDeParser();
                	whereClause.accept(parser);
                	expressionList = parser.getListOfExpressions();
                	
                	//if no AND expression, then just put the original expression to the list
                	if(expressionList.size()==0) {
                		//checking null cos it means I have a where clause with no AND expression.
                		expressionList.add(whereClause.toString());
                	}
            	}
            
            	//now for each individual where clause, parse it so its "left", "right" and "op". 
				List<Map<String,String>> expressionsWithLocation = new ArrayList<Map<String,String>>();
				
				for(String x:expressionList) {
					//extract the left, right and op of each expression and store it as a map, then add the map to the list
					Expression temporaryExpression = CCJSqlParserUtil.parseCondExpression(x);
					MyExpressionVisitor tempVisitor = new MyExpressionVisitor();
					temporaryExpression.accept(tempVisitor);
					expressionsWithLocation.add(tempVisitor.getExpressionMaps());
				}
//				
//				for(Map<String,String>x: expressionsWithLocation) {
//					System.out.println("Exploded: left="+x.get("left")+"  op: "+x.get("op")+"   right="+x.get("right"));
//				}
//				
				
				
				//also the root operator
				Operator finalOperator;
				
				//if join exists, create a left deep join
				if(externalJoins!=null) {
					//initialise a list of join operators
					ArrayList<JoinOperator> listOfJoins = new ArrayList<JoinOperator>();
					
					//create the first join
					SelectOperator s1 = new SelectOperator(firstFrom.toString(),expressionsWithLocation);
					SelectOperator s2 = new SelectOperator(externalJoins.get(0).toString(),expressionsWithLocation);
					JoinOperator firstJoin = new JoinOperator(s1,s2);
					listOfJoins.add(firstJoin);
					
					//then look out for more joins, and carry out the where clauses check for each additional join, then join it
					for(int i=1;i<externalJoins.size();i++){
						SelectOperator dummySelect = new SelectOperator(externalJoins.get(i).toString(),expressionsWithLocation);
						JoinOperator dummy = new JoinOperator(listOfJoins.get(i-1),dummySelect);
						listOfJoins.add(dummy);
					}
					
					//check if we select any specific columns from the final join operator.
					finalOperator = listOfJoins.get(listOfJoins.size()-1);
					if(!printAll) {
						//if it does, call it here
						 finalOperator = new ProjectOperator(finalOperator,selectItems);

					}
					
				}
				else {
					//no join operator so carry out the select
					finalOperator = new SelectOperator(firstFrom.toString(),expressionsWithLocation);
					//if it has a project condition, call it. So no SELECT *
					if(!printAll) {
						finalOperator= new ProjectOperator(finalOperator,selectItems);
						
					}
					
				}
				
				//if there is order by then build it here 
				if(orderByValid) {
					finalOperator = new SortOperator(finalOperator,orderByArray);
				}
				
				//check if query has DISTINCT and built the disticnt operator here
				if(isDistinct) {
					finalOperator = new DuplicateEliminationOperator(finalOperator);
				}
				//System.out.println("Printing results now:");
            //	finalOperator.dump();
				
				//write all the tuples of the final/root operator to the outputFile
            	writeToFile(finalOperator,outputFile);
            	
            }
		}
		catch (Exception e) {
			System.err.println("Exception occurred during initial parsing of "+inputFile);
			e.printStackTrace();
		}
	
	}
	
	/**
	 * Call getNextTuple() of the root operator until null then write it to a file.
	 * @param c
	 * @param outputFile
	 * @throws IOException
	 */
	public static void writeToFile(Operator root, String outputFile) throws IOException {
		Tuple e = root.getNextTuple();
		//open writer
		FileWriter writer = new FileWriter(outputFile);
		int counter=0;
		while(e!=null) {
			//to avoid additional empty line at the end.
			if(counter==0) {
				writer.append(e.toString());
				counter++;
			}
			else {
				writer.append("\n"+e.toString());
			}
			e=root.getNextTuple();
		}
		writer.close();
	}

}
