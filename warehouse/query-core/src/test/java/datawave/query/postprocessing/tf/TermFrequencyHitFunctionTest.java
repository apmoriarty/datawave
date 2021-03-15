package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class TermFrequencyHitFunctionTest {
    
    // Document fully satisfies the function
    @Test
    public void testWithin_fullHit() throws ParseException {
        String query = "content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testWithin_halfHit() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox') || \n"
                        + "(content:within(TEXT, 3,'lazy','dog') && (TEXT == 'lazy' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testWithin_partialHitExcluded() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox') || \n"
                        + "(content:within(TEXT, 3, termOffsetMap, 'lazy','dog') && (TEXT == 'lazy' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox", "the", "lazy"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    // content:within(TEXT_A || TEXT_B, within, termOffsetMap, values...)
    @Test
    public void testWithin_dualFieldFunction_excludeOneField() throws ParseException {
        String query = "(content:within((TEXT_A || TEXT_B), 3, termOffsetMap, 'quick', 'fox') && ((TEXT_A == 'quick' && TEXT_A == 'fox') || (TEXT_B == 'quick' && TEXT_B == 'fox')))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT_A", Arrays.asList("quick", "brown", "fox", "the", "lazy"));
        fieldValues.put("TEXT_B", "fox"); // absence of {TEXT_B,'quick'} causes TEXT_B to be dropped from the hit fields
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_A"));
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testAdjacent_fullHit() throws ParseException {
        String query = "content:adjacent(TEXT, termOffsetMap, 'brown', 'fox') && TEXT == 'brown' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testAdjacent_halfHit() throws ParseException {
        String query = "(content:adjacent(TEXT, termOffsetMap, 'brown', 'fox') && (TEXT == 'brown' && TEXT == 'fox')) || \n"
                        + "(content:adjacent(TEXT, termOffsetMap, 'red', 'dog') && (TEXT == 'red' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testAdjacent_partialHitExcluded() throws ParseException {
        String query = "(content:adjacent(TEXT, termOffsetMap, 'brown', 'fox') && TEXT == 'brown' && TEXT == 'fox') || \n"
                        + "(content:adjacent(TEXT, 'fox','two') && (TEXT == 'fox' && TEXT == 'two'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox", "the", "lazy"));
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    // content:phrase(TEXT_A || TEXT_B, adjacent, termOffsetMap, value_a, value_b)
    @Test
    public void testAdjacent_dualFieldFunction_excludeOneField() throws ParseException {
        String query = "(content:adjacent((TEXT_A || TEXT_B), termOffsetMap, 'quick', 'fox') && ((TEXT_A == 'quick' && TEXT_A == 'fox') || (TEXT_B == 'quick' && TEXT_B == 'fox')))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT_A", "quick");
        fieldValues.put("TEXT_A", "brown");
        fieldValues.put("TEXT_A", "fox");
        fieldValues.put("TEXT_A", "the");
        fieldValues.put("TEXT_A", "lazy");
        fieldValues.put("TEXT_B", "fox"); // absence of {TEXT_B,'quick'} causes TEXT_B to be dropped from the hit fields
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_A"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testPhrase_fullHit() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'quick','brown','fox') && (TEXT == 'quick' && TEXT == 'brown' && TEXT == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "quick");
        fieldValues.put("TEXT", "brown");
        fieldValues.put("TEXT", "fox");
        fieldValues.put("TEXT", "lazy");
        fieldValues.put("TEXT", "dog");
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testPhrase_halfHit() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'quick', 'brown', 'fox') && (TEXT == 'quick' && TEXT == 'brown' && TEXT == 'fox')) || \n"
                        + "(content:phrase(TEXT, termOffsetMap, 'the', 'lazy', 'dog') && (TEXT == 'the' && TEXT == 'lazy' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "quick");
        fieldValues.put("TEXT", "brown");
        fieldValues.put("TEXT", "fox");
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testPhrase_partialHitExcluded() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'quick', 'brown', 'fox') && (TEXT == 'quick' && TEXT == 'brown' && TEXT == 'fox')) || \n"
                        + "(content:phrase(TEXT, termOffsetMap, 'the', 'lazy', 'dog') && (TEXT == 'the' && TEXT == 'lazy' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "quick");
        fieldValues.put("TEXT", "brown");
        fieldValues.put("TEXT", "fox");
        fieldValues.put("TEXT", "the");
        fieldValues.put("TEXT", "lazy"); // absence of 'dog' prunes second phrase
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    // content:phrase(TEXT, A, A, B)
    @Test
    public void testPhrase_repeatedTerm() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'knock', 'knock') && (TEXT == 'knock'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "it's");
        fieldValues.put("TEXT", "a");
        fieldValues.put("TEXT", "knock");
        fieldValues.put("TEXT", "knock");
        fieldValues.put("TEXT", "joke"); // absence of 'dog' prunes second phrase
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0knock\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    // content:phrase(TEXT_A || TEXT_B, adjacent, termOffsetMap, value_a, value_b)
    @Test
    public void testPhrase_dualFieldFunction_excludeOneField() throws ParseException {
        String query = "(content:phrase((TEXT_A || TEXT_B), termOffsetMap, 'quick','brown','fox') && "
                        + "((TEXT_A == 'quick' && TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'quick' && TEXT_B == 'brown' && TEXT_B == 'fox')))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT_A", "quick");
        fieldValues.put("TEXT_A", "brown");
        fieldValues.put("TEXT_A", "fox");
        fieldValues.put("TEXT_A", "lazy");
        fieldValues.put("TEXT_A", "dog");
        fieldValues.put("TEXT_B", "brown");
        fieldValues.put("TEXT_B", "dog");
        fieldValues.put("TEXT_B", "fox");
        fieldValues.put("TEXT_B", "two");
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT_A"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testPhraseAndAdjacent_bothHit() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'quick','brown','fox') && (TEXT == 'quick' && TEXT == 'brown' && TEXT == 'fox')) || \n"
                        + "(content:adjacent(TEXT, termOffsetMap, 'lazy','dog') && (TEXT == 'lazy' && TEXT == 'dog'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "quick");
        fieldValues.put("TEXT", "brown");
        fieldValues.put("TEXT", "fox");
        fieldValues.put("TEXT", "lazy");
        fieldValues.put("TEXT", "dog");
        
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0lazy\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0dog\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    @Test
    public void testIntersectingChildren() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'brown','fox') && (TEXT == 'quick' && TEXT == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        // brown hits on uid0, uid1. fox hits on uid1, uid2.
        Document doc = buildDocumentWithIntersectingChildren();
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid1\0brown\0TEXT"));
        expectedHits.add(new Text("datatype\0uid1\0fox\0TEXT"));
        
        test(script, doc, expectedHits);
    }
    
    // Test a hit on numeric, no-op
    @Test
    public void testHitsAgainstMultipleTypes() throws ParseException {
        
        String query = "content:phrase(TEXT, termOffsetMap, '1', 'ping', 'only') && TEXT == '1' && TEXT == 'ping' && TEXT == 'only'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "1");
        fieldValues.put("TEXT", "ping");
        fieldValues.put("TEXT", "only");
        
        Key docKey = new Key("shard", "datatype\0uid0");
        Document doc = buildDocument(fieldValues);
        
        TermFrequencyHitFunction hitFunction = new TermFrequencyHitFunction(script, fieldValues);
        TreeSet<Text> hits = hitFunction.apply(docKey, doc);
        
        // Expected hits
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\u0000uid0\u00001\u0000TEXT"));
        expectedHits.add(new Text("datatype\u0000uid0\u0000ping\u0000TEXT"));
        expectedHits.add(new Text("datatype\u0000uid0\u0000only\u0000TEXT"));
        
        assertEquals(expectedHits, hits);
    }
    
    @Test
    public void testFunction() throws ParseException {
        String query = "content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("TEXT", "quick");
        fieldValues.put("TEXT", "brown");
        fieldValues.put("TEXT", "fox");
        fieldValues.put("TEXT", "the");
        fieldValues.put("TEXT", "lazy");
        
        Document doc = buildDocument(fieldValues);
        
        TermFrequencyHitFunction hitFunction = new TermFrequencyHitFunction(script, fieldValues);
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> hits = hitFunction.apply(docKey, doc);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        
        assertEquals(expectedHits, hits);
    }
    
    @Test
    public void testFunction_fieldNotInFunction() throws ParseException {
        String query = "content:within(3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.putAll("TEXT", Arrays.asList("quick", "brown", "fox", "the", "lazy"));
        
        Document doc = buildDocument(fieldValues);
        
        TermFrequencyHitFunction hitFunction = new TermFrequencyHitFunction(script, fieldValues);
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> hits = hitFunction.apply(docKey, doc);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        
        assertEquals(expectedHits, hits);
    }
    
    @Test
    public void testDelayedContentFunction() throws ParseException {
        String query = "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("FOO", "bar");
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits, createSource());
    }
    
    // Exclude hit is in tld
    @Test
    public void testNegatedContentFunction_ExcludeInTld() throws ParseException {
        String query = "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("FOO", "bar");
        Document doc = buildDocument(fieldValues);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT"));
        
        test(script, doc, expectedHits, createSource());
    }
    
    // Exclude hit is in uid.1
    @Test
    public void testNegatedContentFunction_ExcludeInChildDoc() throws ParseException {
        String query = "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Multimap<String,String> fieldValues = HashMultimap.create();
        fieldValues.put("FOO", "bar");
        Document doc = buildDocument(fieldValues);
        
        List<Map.Entry<Key,Value>> fiData = new ArrayList<>();
        fiData.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid0.1"), new Value()));
        fiData.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "quick\0datatype\0uid0.1"), new Value()));
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0quick\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0fox\0TEXT"));
        
        test(script, doc, expectedHits, createSource(fiData), true);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected) {
        test(script, doc, expected, false);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, boolean isTld) {
        test(script, doc, expected, null, isTld);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source) {
        test(script, doc, expected, source, false);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source, boolean isTld) {
        TermFrequencyHitFunction hitFunction = new TermFrequencyHitFunction(script, null);
        hitFunction.setSource(source);
        hitFunction.setIsTld(isTld);
        
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> hits = hitFunction.apply(docKey, doc);
        
        assertEquals(expected, hits);
    }
    
    private Document buildDocument(Multimap<String,String> fieldValues) {
        Key docKey = new Key("shard", "datatype\0uid0");
        Document d = new Document();
        for (String field : fieldValues.keySet()) {
            Collection<String> values = fieldValues.get(field);
            for (String value : values) {
                // The docKey is actually the raw key, post aggregation. Includes the child id.
                PreNormalizedAttribute attr = new PreNormalizedAttribute(value, docKey, true);
                d.put(field, attr);
            }
        }
        return d;
    }
    
    // Hand craft event with intersecting children
    private Document buildDocumentWithIntersectingChildren() {
        Document d = new Document();
        d.put("TEXT", new PreNormalizedAttribute("brown", new Key("shard", "datatype\0uid0"), true));
        d.put("TEXT", new PreNormalizedAttribute("brown", new Key("shard", "datatype\0uid1"), true));
        d.put("TEXT", new PreNormalizedAttribute("fox", new Key("shard", "datatype\0uid1"), true));
        d.put("TEXT", new PreNormalizedAttribute("fox", new Key("shard", "datatype\0uid2"), true));
        return d;
    }
    
    private SortedKeyValueIterator<Key,Value> createSource() {
        // Field Index source
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "quick\0datatype\0uid0"), new Value()));
        return new SortedListKeyValueIterator(data);
    }
    
    private SortedKeyValueIterator<Key,Value> createSource(List<Map.Entry<Key,Value>> data) {
        return new SortedListKeyValueIterator(data);
    }
}
