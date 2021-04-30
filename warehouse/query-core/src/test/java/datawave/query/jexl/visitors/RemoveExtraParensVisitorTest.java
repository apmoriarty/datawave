package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoveExtraParensVisitorTest {
    
    @Test
    public void testSimpleCase() throws ParseException {
        String query = "((FOO == 'bar'))";
        String expected = "(FOO == 'bar')";
        test(query, expected);
    }
    
    @Test
    public void testLotsOfWraps() throws ParseException {
        String query = "((((((((((((((((((FOO == 'bar'))))))))))))))))))";
        String expected = "(FOO == 'bar')";
        test(query, expected);
    }
    
    @Test
    public void testLessThanSimpleCase() throws ParseException {
        String query = "(((((FOO == 'bar')) || (FOO2 == 'bar2'))))";
        String expected = "((FOO == 'bar') || (FOO2 == 'bar2'))";
        test(query, expected);
    }
    
    private void test(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        ASTJexlScript visited = (ASTJexlScript) RemoveExtraParensVisitor.remove(script);
        
        String visitedString = JexlStringBuildingVisitor.buildQueryWithoutParse(visited);
        assertEquals(expected, visitedString);
        
        assertLineage(visited);
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
