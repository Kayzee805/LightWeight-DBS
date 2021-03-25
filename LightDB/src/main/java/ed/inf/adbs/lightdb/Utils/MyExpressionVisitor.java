package ed.inf.adbs.lightdb.Utils;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;

/**
 * MyExpressionVisiter class, will visit each expression that is free of AND expressions.
 * Then return a map that contains "left", "right" and "op", and the values coressponding to them.
 * @author kayze
 *
 */
/*
 * https://stackoverflow.com/questions/46800058/fully-parsing-where-clause-with-jsqlparser
 * Similar approach to this stack overflow post.
 */
public class MyExpressionVisitor extends ExpressionVisitorAdapter {
	
	//store the map of all left,op and right expressions of a where clause as string
	Map<String, String> expressionMaps = new HashMap<String,String>();
	

	/**
	 * Visit an expression then parse it to left, op and right values and store it the map.
	 */
	@Override
	protected void visitBinaryExpression(BinaryExpression expr) {
        if (expr instanceof ComparisonOperator) {
        	expressionMaps.put("left",expr.getLeftExpression().toString());
        	expressionMaps.put("op",expr.getStringExpression().toString());
        	expressionMaps.put("right",expr.getRightExpression().toString());
        }

        super.visitBinaryExpression(expr); 
	}
	
	/**
	 * Getter for the expression map.
	 * @return expressionMaps
	 */
	public Map<String,String> getExpressionMaps(){
		return expressionMaps;
	}

	
}
