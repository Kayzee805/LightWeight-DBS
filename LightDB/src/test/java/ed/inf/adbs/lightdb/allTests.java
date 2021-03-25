package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;


public class allTests {
	private static String seperator = File.separator;
	private static String rootPath= "samples"+seperator;

	@Test
	public void test() throws IOException {
		String[] queryNames = new String[] {
	            "Scan_all",
                "Select_1",
                "Select_2",
                "Select_3",
                "Select_4",
                "Select_impossible",
                "Project_1",
                "Project_2",
                "Join_1",
                "Join_2",
                "Join_3",
                "Join_withSelect1",
                "Join_withSelect2",
                "Join_nullExpression1",
                "Join_nullExpression2",
                "Join_SelfJoin1",
                "Join_SelfJoin2",
                "Alias_1",
                "Alias_2",
                "Select_withAlias1",
                "Join_oneBaseTable_oneAlias",
                "OrderBy_1",
                "OrderBy_withJoin1",
                "OrderBy_WithJoin2",
                "OrderBy_nullRelation",
                "Distinct_withoutOrderBy1", // this might falsely fail if you implemented your "Distinct without Order By" differently
                "Distinct_withoutOrderBy2", // this might falsely fail if you implemented your "Distinct without Order By" differently
                "Distinct_withOrderBy1",
                "Distinct_emptyRelation",
                "Distinct_withAlias1",
                "NullOutput",
                "NullTable1",
                "NullTable2",
                "Negative_Numbers",
                "Negative_Numbers_Empty",
                "Negative_Numbers_Positive",
                "Negative_Numbers_And_Columns",
                "Equal",
                "Not_Equal",
                "GreaterThanEqual",
                "LessThanEqual",
                "Negative_Table",
                "Negative_Table2"


		};
		
		boolean allPassed =true;
		for(String x:queryNames) {
			//System.out.println("Starting "+x);
			boolean passed = testQuery(x);
			if(!passed) {
				System.err.println("Failed query "+x);
				allPassed=false;
			}
			else {
				System.out.println("Passed query "+x);
			}
		}
		Assert.assertTrue(allPassed);
	}
	
	
	public boolean testQuery(String query) throws IOException{
        String dbPath = rootPath + "db";
        String inputPath = rootPath + "input" +seperator+ query + ".sql";
        String outPath = rootPath + "output" +seperator+ query + ".csv";
        String expectedOutPath = rootPath + "expected_output"+seperator + query + ".csv";		
        
        String[] args = new String[] {dbPath, inputPath, outPath};
        try {
            LightDB.main(args);
        } catch (Exception e) {
            return false;
        }
        byte[] f1 = Files.readAllBytes(Paths.get(outPath));
        byte[] f2 = Files.readAllBytes(Paths.get(expectedOutPath));

        if (f1.length == 0 && f2.length != 0)
            return false;

        if (f1.length < f2.length)
            f2 = Arrays.copyOf(f2, f1.length);
        else if (f1.length > f2.length)
            f1 = Arrays.copyOf(f1, f2.length);

   //    System.out.println("Arr1: " + Arrays.toString(f1)+" "+query);
    //    System.out.println("Arr2: " + Arrays.toString(f2)+" "+query);
        return Arrays.equals(f1, f2);
        
	}

}
