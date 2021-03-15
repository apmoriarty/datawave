package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Function that determines the search space for a query's content functions given a document.
 *
 * The search space is a set of fully qualified TermFrequency column qualifiers
 */
public class TermFrequencyHitFunction {
    
    private final Logger log = Logger.getLogger(TermFrequencyHitFunction.class);
    
    // The fields to get from the document
    private final Set<String> functionFields = new HashSet<>();
    
    // The field\0value pairs that define a functions search space. Used to filter the document.
    private final Multimap<Function,String> functionSearchSpace = HashMultimap.create();
    
    private final Map<Function,FunctionArgs> functionArgsCache = new HashMap<>();
    
    // If a content function does not have a field in it's argument list, fall back to this map.
    private final Multimap<String,String> tfFVs;
    
    private boolean hasNegatedFunctions;
    
    // Document built from any negated content function terms that hit
    private Document negatedDocument;
    
    private SortedKeyValueIterator<Key,Value> source;
    
    // use this one weird trick to avoid an obnoxious number of source deep copies!
    private DelayedFieldIndexIterator iter;
    
    // Determines how the seek range for the field index is built, i.e., should it consider child documents
    private boolean isTld = false;
    
    public TermFrequencyHitFunction(ASTJexlScript script, Multimap<String,String> tfFVs) {
        this.tfFVs = tfFVs;
        populateFunctionSearchSpace(script);
    }
    
    public void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    public void setIsTld(boolean isTld) {
        this.isTld = isTld;
    }
    
    private void initializeSource() throws IOException {
        iter = new DelayedFieldIndexIterator();
        iter.init(source, null, null);
    }
    
    /**
     * Build the search space for all content phrase functions that contributed to this document hit
     *
     * @param docKey
     *            a key taking the form shard:datatype\0uid
     * @param doc
     *            an aggregated document
     * @return a sorted set of TermFrequency column qualifiers (datatype\0uid\0value\0field)
     */
    public TreeSet<Text> apply(Key docKey, Document doc) {
        TreeSet<Text> hits = new TreeSet<>();
        // Track the document hits as FIELD -> field\0value
        Multimap<String,String> documentHits = HashMultimap.create();
        // Track the mapping of field/value to uids, specifically for the TLD case
        Multimap<String,String> vfUidCache = HashMultimap.create();
        
        // 0. Handle negated content functions first.
        if (hasNegatedFunctions) {
            if (isTld) {
                // A negated field value could be in any child document. Must scan the fi and build a document of those hits.
                try {
                    negatedDocument = buildNegatedDocument(docKey);
                } catch (IOException e) {
                    log.error("Problem building negated document: ", e);
                }
            } else {
                // In the event query case we only have one uid, thus we know where to find the negated terms -- if they exist.
                buildSearchSpaceFromNegatedFunctions(docKey, documentHits, vfUidCache);
            }
        }
        
        // 1. Filter the document by the content function search space
        for (String field : functionFields) {
            Attribute<?> attribute = doc.get(field);
            buildSearchSpaceFromDocument(field, attribute, documentHits, vfUidCache);
            
            if (negatedDocument != null) {
                attribute = negatedDocument.get(field);
                buildSearchSpaceFromDocument(field, attribute, documentHits, vfUidCache);
            }
        }
        
        // 2. Now filter the function search space via the document hits
        // At this point terms have been fetched for both inclusive and exclusive content function terms.
        for (Function function : functionSearchSpace.keySet()) {
            FunctionArgs args = functionArgsCache.get(function);
            
            // Compute the function field-value pairs on the fly...
            for (String field : args.fields) {
                Set<String> functionHitsForField = new HashSet<>();
                for (String value : args.values) {
                    String vf = value + '\u0000' + field;
                    if (documentHits.containsEntry(field, vf)) {
                        functionHitsForField.add(vf);
                    }
                }
                
                // If any pair was filtered out continue to the next function
                if (functionHitsForField.size() != args.values.size()) {
                    continue;
                }
                
                // Perform iterative intersection
                Set<String> intersectedUids = new HashSet<>();
                if (!functionHitsForField.isEmpty()) {
                    
                    Iterator<String> vfIter = functionHitsForField.iterator();
                    intersectedUids.addAll(vfUidCache.get(vfIter.next()));
                    
                    while (vfIter.hasNext()) {
                        
                        Set<String> nextUids = (Set<String>) vfUidCache.get(vfIter.next());
                        intersectedUids = Sets.intersection(intersectedUids, nextUids);
                        
                        if (intersectedUids.isEmpty()) {
                            // If we prune to zero at any point, stop for this field.
                            functionHitsForField.clear();
                            break;
                        }
                    }
                }
                
                // Build TF CQ like 'datatype\0uid\0value\0field'
                if (!functionHitsForField.isEmpty()) {
                    
                    String datatype = parseDatatypeFromCF(docKey);
                    
                    for (String uid : intersectedUids) {
                        for (String valueField : functionHitsForField) {
                            hits.add(new Text(datatype + '\u0000' + uid + '\u0000' + valueField));
                        }
                    }
                }
            }
        }
        
        return hits;
    }
    
    /**
     * For the case of an event query with negated content functions, build the search space without reaching back to the field index.
     *
     * @param docKey
     *            a key like shard:datatype\0uid
     * @param documentHits
     *            tracks the value hits for a field
     * @param vfUidCache
     *            tracks the uids that map to a specific value\0field pair
     */
    private void buildSearchSpaceFromNegatedFunctions(Key docKey, Multimap<String,String> documentHits, Multimap<String,String> vfUidCache) {
        String uid = parseUidFromCF(docKey);
        for (FunctionArgs args : functionArgsCache.values()) {
            if (args.isNegated) {
                for (String field : args.fields) {
                    for (String value : args.values) {
                        String valueField = value + '\u0000' + field;
                        if (functionSearchSpace.containsValue(valueField)) {
                            documentHits.put(field, valueField);
                            // populate uid cache.
                            vfUidCache.put(valueField, uid);
                        }
                    }
                }
            }
        }
    }
    
    private void buildSearchSpaceFromDocument(String field, Attribute<?> attribute, Multimap<String,String> documentHits, Multimap<String,String> vfUidCache) {
        if (attribute instanceof Attributes) {
            Attributes attrs = (Attributes) attribute;
            Set<Attribute<?>> attrSet = attrs.getAttributes();
            for (Attribute<?> element : attrSet) {
                
                // shard:datatype\0uid
                Key metadata = element.getMetadata();
                
                String uid = parseUidFromCF(metadata);
                
                String value;
                Object data = element.getData();
                if (data instanceof String) {
                    value = (String) data;
                } else if (data instanceof Type<?>) {
                    Type<?> type = (Type<?>) data;
                    value = type.getNormalizedValue();
                } else {
                    throw new IllegalStateException("Expected attribute to be either a String or a Type<?> but was " + data.getClass());
                }
                
                // Note: using value\0field is easier when building the TermFrequency column qualifier at the end
                String valueField = value + '\u0000' + field;
                if (functionSearchSpace.containsValue(valueField)) {
                    documentHits.put(field, valueField);
                    
                    // populate uid cache.
                    vfUidCache.put(valueField, uid);
                }
            }
        }
        // If the attribute was a single instance there is no point in evaluating it.
        // ContentFunctions by definition require more than one hit.
    }
    
    /**
     * Build up a document of negated content function terms. Uses the {@link DelayedFieldIndexIterator} to fetch the uids for a field value pair off of the
     * field index.
     *
     * Tracks which field value pairs have already been run, that is which returned results or returned zero results. This information avoids duplicate work and
     * can exclude entire content functions that share a field value that has no entries in the field index.
     *
     * @param docKey
     *            the document key
     * @return a document of negated field value pairs
     * @throws IOException
     */
    private Document buildNegatedDocument(Key docKey) throws IOException {
        
        long start = System.nanoTime();
        
        Document doc = new Document();
        Range limitRange = buildLimitRange(docKey);
        
        // Track field value pairs that returned hits, or returned nothing.
        Multimap<String,String> fetchedCache = HashMultimap.create();
        Multimap<String,String> missedCache = HashMultimap.create();
        
        for (FunctionArgs args : functionArgsCache.values()) {
            // Only lookup field values if the content function is under a negation
            if (!args.isNegated) {
                continue;
            }
            
            for (String field : args.fields) {
                // Check to see if this sub query contains a term that missed during a previous lookup
                if (missedCache.containsKey(field)) {
                    boolean found = false;
                    for (String value : args.values) {
                        if (missedCache.containsEntry(field, value)) {
                            // One of the terms in this function previously missed, returning zero results.
                            // Do not bother looking up any field-values for this functions
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        // Skip this sub query
                        continue;
                    }
                }
                
                for (String value : args.values) {
                    
                    // Do not lookup the same field-value pair more than once
                    if (fetchedCache.containsEntry(field, value)) {
                        continue;
                    }
                    
                    // Fetch keys for this node
                    boolean fetchedValues = fetchKeysForFieldValue(doc, limitRange, field, value);
                    if (!fetchedValues) {
                        // if this field-value had nothing in the field index, we're done with this sub query.
                        missedCache.put(field, value);
                        break;
                    } else {
                        fetchedCache.put(field, value);
                    }
                }
            }
        }
        
        if (log.isDebugEnabled()) {
            long total = System.nanoTime() - start;
            log.debug("Time to build negated document: " + (total / 1000000) + " ms, " + total + " ns.");
        }
        
        return doc;
    }
    
    /**
     * Fetch field value pairs from the field index using a delayed iterator
     *
     * @param doc
     *            fetched keys added to this document
     * @param limitRange
     *            used to build the seek range
     * @param field
     *            the field
     * @param value
     *            the value
     * @return true if this field value pair contained entries in the field index
     * @throws IOException
     */
    private boolean fetchKeysForFieldValue(Document doc, Range limitRange, String field, String value) throws IOException {
        
        if (iter == null) {
            // lazy init
            initializeSource();
        }
        
        Range seekRange = buildSeekRangeForFi(limitRange.getStartKey(), field, value);
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0" + field));
        iter.seek(seekRange, seekCFs, true);
        
        int keysFetched = 0;
        while (iter.hasTop()) {
            keysFetched++;
            Key next = transformKey(iter.getTopKey(), field, value);
            Attribute<?> attr = new PreNormalizedAttribute(value, next, true);
            doc.put(field, attr);
            iter.next();
        }
        
        return keysFetched > 0;
    }
    
    /**
     * Transform a raw field index key into a document key
     *
     * @param key
     *            a field index formatted key
     * @param field
     *            the field
     * @param value
     *            the value
     * @return a document formatted key
     */
    private Key transformKey(Key key, String field, String value) {
        String cqStr = key.getColumnQualifier().toString();
        int index = cqStr.indexOf('\u0000');
        int nextIndex = cqStr.indexOf('\u0000', index + 1);
        
        String datatype = cqStr.substring(index + 1, nextIndex);
        String uid = cqStr.substring(nextIndex + 1);
        
        return new Key(key.getRow(), new Text(datatype + "\0" + uid), new Text(field + "\0" + value));
    }
    
    // return shard:row:datatype\0uid
    private Range buildLimitRange(Key docKey) {
        return new Range(docKey, true, docKey.followingKey(PartialKey.ROW_COLFAM), false);
    }
    
    /**
     * Build a seek range for the field index
     *
     * @param docKey
     *            takes the form 'shard:datatype\0uid
     * @param field
     *            the field
     * @param value
     *            the value
     * @return a seek range built for the field index in hte form 'shard:fi\0field:value\0datatype\0uid
     */
    private Range buildSeekRangeForFi(Key docKey, String field, String value) {
        String cfStr = docKey.getColumnFamily().toString();
        int index = cfStr.indexOf('\u0000');
        String datatype = cfStr.substring(0, index);
        String uid = cfStr.substring(index + 1);
        
        // FI key is shard:fi\0FIELD:value\0datatype\0uid
        Text cf = new Text("fi\u0000" + field);
        Text cq = new Text(value + '\u0000' + datatype + '\u0000' + uid);
        
        Key startKey = new Key(docKey.getRow(), cf, cq);
        Key endKey;
        if (isTld) {
            // Append max unicode to pick up child uids
            cq = new Text(value + '\u0000' + datatype + '\u0000' + uid + Constants.MAX_UNICODE_STRING);
            endKey = new Key(docKey.getRow(), cf, cq);
        } else {
            // restrict search to just this specific uid
            endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
        
        return new Range(startKey, true, endKey, false);
    }
    
    // Will recursively ascend the tree looking for an ASTDelayedPredicate
    private boolean isDelayed(Function function) {
        JexlNode arg = function.args().get(0);
        return isDelayed(arg, new HashSet<>());
    }
    
    // recursively ascend a query tree looking for ASTDelayedPredicate nodes
    private boolean isDelayed(JexlNode node, Set<JexlNode> seen) {
        
        seen.add(node);
        
        if (node instanceof ASTJexlScript) {
            return false;
        } else if (node instanceof ASTAndNode) {
            // If a child is an instance of an ASTDelayedPredicate
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                if (!seen.contains(child) && ASTDelayedPredicate.instanceOf(child)) {
                    return true;
                }
            }
            
            // If no child was delayed, continue ascending
            return isDelayed(node.jjtGetParent(), seen);
        } else if (ASTDelayedPredicate.instanceOf(node)) {
            return true;
        } else {
            return isDelayed(node.jjtGetParent(), seen);
        }
    }
    
    // parse the uid directly from a key with a column family like datatype\0uid
    private String parseUidFromCF(Key k) {
        ByteSequence backing = k.getColumnFamilyData();
        int index = -1;
        for (int i = backing.offset(); i < backing.length(); i++) {
            if (backing.byteAt(i) == '\u0000') {
                index = i + 1;
                break;
            }
        }
        return new String(backing.subSequence(index, backing.length()).toArray());
    }
    
    // parse the datatype directly from a key with a column family like datatype\0uid
    private String parseDatatypeFromCF(Key k) {
        ByteSequence backing = k.getColumnFamilyData();
        int index = -1;
        for (int i = backing.offset(); i < backing.length(); i++) {
            if (backing.byteAt(i) == '\u0000') {
                index = i;
                break;
            }
        }
        return new String(backing.subSequence(0, index).toArray());
    }
    
    /**
     * Preprocessing step
     * 
     * @param script
     *            the query tree
     */
    private void populateFunctionSearchSpace(ASTJexlScript script) {
        Multimap<String,Function> allFunctions = TermOffsetPopulator.getContentFunctions(script);
        Set<Function> functions = new HashSet<>(allFunctions.values());
        for (Function function : functions) {
            FunctionArgs args = parseFunction(function);
            // update function arg cache
            functionArgsCache.put(function, args);
            
            // populate field search space
            functionFields.addAll(args.fields);
            
            // populate field/value search space
            for (String field : args.fields) {
                for (String value : args.values) {
                    // Easier to build TF CQs if done this way up front
                    String valueField = value + '\u0000' + field;
                    functionSearchSpace.put(function, valueField);
                }
            }
        }
    }
    
    /**
     * Get this function's search space as defined by fields and values
     *
     * @param function
     *            a {@link Function} that is either adjacent, phrase, or within
     * @return a list of fields and values associated with the provided function
     */
    private FunctionArgs parseFunction(Function function) {
        // functions may take different forms..
        // within = {field, number, termOffsetMap, terms...}
        // within = {number, termOffsetMap, terms...}
        // adjacent = {field, termOffsetMap, terms...}
        // adjacent = {termOffsetMap, terms...}
        // phrase = {field, termOffsetMap, terms...}
        // phrase = {termOffsetMap, terms...}
        List<JexlNode> args = function.args();
        int index = function.name().equals("within") ? 3 : 2;
        
        // If the first arg is a number or termOffsetMap then no field was provided as part of the function.
        // The fields will be build via value lookups from the tfFVs.
        boolean specialCase = false;
        JexlNode first = args.get(0);
        if (isFirstNodeSpecial(first)) {
            specialCase = true;
            index--;
        }
        
        // Parse the values first. Might have to lookup fields by value.
        Set<String> values = getValuesFromArgs(args, index);
        
        Set<String> fields;
        if (specialCase) {
            // field(s) were not present in the function, lookup fields by value.
            fields = lookupFieldsByValues(values);
        } else {
            // function's first arg is the field, or fields in the form (FIELD_A || FIELD_B)
            fields = parseField(args.get(0));
        }
        
        // delayed = negated
        boolean isNegated = isDelayed(function);
        if (isNegated) {
            this.hasNegatedFunctions = true;
        }
        
        return new FunctionArgs(isNegated, fields, values);
    }
    
    // If a function does not contain the fields being queried, find the fields via the term frequency field-value map
    private Set<String> lookupFieldsByValues(Set<String> values) {
        Set<String> fields = new HashSet<>();
        for (String key : tfFVs.keySet()) {
            for (String value : values) {
                if (tfFVs.containsEntry(key, value)) {
                    fields.add(key);
                }
            }
        }
        return fields;
    }
    
    // A node is special if it is not the fields being searched. That is, the first node is a number or a variable 'termOffsetMap'
    private boolean isFirstNodeSpecial(JexlNode node) {
        if (node instanceof ASTNumberLiteral) {
            return true;
        } else if (node instanceof ASTReference) {
            List<ASTIdentifier> ids = JexlASTHelper.getIdentifiers(node);
            if (ids.size() == 1 && ids.get(0).image.equals("termOffsetMap")) {
                return true;
            }
        }
        return false;
    }
    
    private Set<String> parseField(JexlNode node) {
        Set<String> fields = new HashSet<>();
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            fields.add(identifier.image);
        }
        return fields;
    }
    
    /**
     * Parse the values out of a content functions arguments
     *
     * @param args
     *            content function arguments as a list of Jexl nodes
     * @param start
     *            the start index for the values
     * @return a list of normalized string values
     */
    private Set<String> getValuesFromArgs(List<JexlNode> args, int start) {
        Set<String> values = new HashSet<>();
        for (int i = start; i < args.size(); i++) {
            List<String> parsed = parseArg(args.get(i));
            values.addAll(parsed);
        }
        return values;
    }
    
    private List<String> parseArg(JexlNode node) {
        List<String> parsed = new LinkedList<>();
        List<Object> values = JexlASTHelper.getLiteralValues(node);
        for (Object value : values) {
            if (value instanceof String) {
                parsed.add((String) value);
            } else {
                throw new IllegalStateException("Expected literal value to be String cast-able");
            }
        }
        return parsed;
    }
    
    // utility class
    private class FunctionArgs {
        boolean isNegated;
        Set<String> fields;
        Set<String> values;
        
        public FunctionArgs(boolean isNegated, Set<String> fields, Set<String> values) {
            this.isNegated = isNegated;
            this.fields = fields;
            this.values = values;
        }
    }
}
