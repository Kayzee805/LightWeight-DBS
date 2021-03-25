package ed.inf.adbs.lightdb.Utils;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * MyExpressionDeParser class which will visit different where clauses 
 * if there exists and "AND" expression
 * @author kayze
 *
 */
/*
 * https://stackoverflow.com/questions/43902141/jsqlparser-pretty-print-where-clause
 * Similar approach to this stack overflow post.
 */
public class MyExpressionDeParser extends ExpressionDeParser{

	//will store the list of expressions as map of keys: left, op and right
	private List<Map<String,String>> listOfExpressions = new ArrayList<Map<String,String>>();
	
	/*
	 * Visit any AND expressions
	 */
    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpr(andExpression, "AND");
    }
    
    /**
     * For each and expressions, get the left and right expressions
     * then store it in the map
     * @param expr
     * @param operator
     */
    private void visitBinaryExpr(BinaryExpression expr, String operator) {
    	Map<String,String> oneExpression = new HashMap<String,String>();
    	
    	if(!(expr.getLeftExpression() instanceof AndExpression)) {
    		getBuffer();
    	}
    	expr.getLeftExpression().accept(this);
    	oneExpression.put("left",expr.getLeftExpression().toString());
    	oneExpression.put("op",operator);
    	if(!(expr.getRightExpression() instanceof AndExpression)) {
    		getBuffer();
    	}
    	expr.getRightExpression().accept(this);
    	oneExpression.put("right",expr.getRightExpression().toString());
    	listOfExpressions.add(oneExpression);
    }
    
    
    /**
     * For each map in the list of expressions (left,op,right)
     * Separate it to individual expressions and return these expressions 
     * as a list of string.
     * @return
     */
    public List<String> getListOfExpressions(){
    	List<String> expressionAsString= new ArrayList<String>();
    	/*
    	 * The map will contain subexpressions as well
    	 * So, say: where a>1 and a<5 and 1=3
    	 * the list will be of size 2
    	 * with first map being, left = a>1 and right a<5
    	 * the 2nd map will be: left = a>1 and a<5 then the right is 1=3
    	 */
    	
    	for(int i=0;i<listOfExpressions.size();i++) {
    		
    		/*
    		 * if its the first expression, we add both the left and right expressions as 
    		 * both of it will be individual expressions.
    		 * Where as, later left expressions will contain multiple expressions and right will
    		 * be a single expression. The left will be a union of all sub expressions.
    		 */
    		
    		if(i==0) {
    			expressionAsString.add(listOfExpressions.get(i).get("left"));
    			expressionAsString.add(listOfExpressions.get(i).get("right"));
    		}
    		else {
    			expressionAsString.add(listOfExpressions.get(i).get("right"));
    		}
    	}
    	return expressionAsString;
    }
}
