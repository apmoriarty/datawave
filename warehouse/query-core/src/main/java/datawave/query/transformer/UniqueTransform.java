package datawave.query.transformer;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attributes;
import datawave.query.model.QueryModel;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.StringTuple;
import datawave.query.util.Tuple2;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * This is a iterator that will filter documents base on a uniqueness across a set of configured fields. Only the first instance of an event with a unique set
 * of those fields will be returned.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(GroupingTransform.class);
    
    private BloomFilter<byte[]> bloom = null;
    private HashSet<ByteSequence> seen;
    private Set<String> fields;
    private Multimap<String,String> modelMapping;
    private final boolean DEBUG = false;
    
    public UniqueTransform(Set<String> fields) {
        this.fields = fields;
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (DEBUG) {
            this.seen = new HashSet<ByteSequence>();
        }
        if (log.isTraceEnabled())
            log.trace("unique fields: " + this.fields);
    }
    
    /**
     * If passing the logic in, then the model being used by the logic then capture the reverse field mapping
     *
     * @param logic
     * @param fields
     */
    public UniqueTransform(BaseQueryLogic<Entry<Key,Value>> logic, Set<String> fields) {
        this(fields);
        QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
            }
        }
    }
    
    public Predicate<Entry<Key,Document>> getUniquePredicate() {
        return new Predicate<Entry<Key,Document>>() {
            @Override
            public boolean apply(@Nullable Entry<Key,Document> input) {
                return UniqueTransform.this.apply(input) != null;
            }
        };
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            try {
                if (isDuplicate(keyDocumentEntry.getValue())) {
                    keyDocumentEntry = null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
        return keyDocumentEntry;
    }
    
    private byte[] getBytes(Document document) throws IOException {
        // we need to pull the fields out of the document.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        List<FieldSet> fieldSets = getOrderedFieldSets(document);
        int count = 0;
        for (FieldSet fieldSet : fieldSets) {
            String separator = "f" + (count++) + ":";
            for (Map.Entry<String, String> entry : fieldSet.entrySet()) {
                output.writeChars(separator);
                output.writeChars(entry.getKey());
                output.writeChar('=');
                output.writeChars(entry.getValue());
                separator=",";
            }
        }
        output.flush();
        return bytes.toByteArray();
    }

    private static class FieldSet extends TreeMap<String, String> implements Comparable<FieldSet> {

        @Override
        public int compareTo(FieldSet o) {
            Iterator<Map.Entry<String, String>> theseKeys = entrySet().iterator();
            Iterator<Map.Entry<String, String>> thoseKeys = o.entrySet().iterator();
            int comparison = 0;
            while(comparison == 0 && theseKeys.hasNext() && thoseKeys.hasNext()) {
                Map.Entry<String, String> thisKey = theseKeys.next();
                Map.Entry<String, String> thatKey = thoseKeys.next();
                comparison = thisKey.getKey().compareTo(thatKey.getKey());
                if (comparison == 0) {
                    comparison = thisKey.getValue().compareTo(thatKey.getValue());
                }
            }
            if (comparison == 0) {
                if (theseKeys.hasNext()) {
                    return 1;
                } else if (thoseKeys.hasNext()) {
                    return -1;
                }
            }
            return comparison;
        }
    }

    private List<FieldSet> getOrderedFieldSets(Document document) {
        Set<Multimap<String, String>> fieldSets = getFieldSets(document);
        List<FieldSet> orderedFieldSets = new ArrayList<>(fieldSets.size());
        for (Multimap<String, String> fieldSet : fieldSets) {
            FieldSet orderedFieldSet = new FieldSet();
            for (String field : fieldSet.keySet()) {
                List<String> values = new ArrayList<>(fieldSet.get(field));
                Collections.sort(values);
                String value = Joiner.on('\0').join(values);
                orderedFieldSet.put(field, value);
            }
            orderedFieldSets.add(orderedFieldSet);
        }
        Collections.sort(orderedFieldSets);
        return orderedFieldSets;
    }

    /**
     * This will return attributes from a document that uniquely identify this document for a set of fields.
     * The attributes will be organized as an ordered set of ordered attribute sets.
     *
     * Definitions using example of "field.a.b.x = y"
     *   fieldname: "field"
     *   grouping context: "a.b.x"
     *   value: "y"
     * The unique fields to be grouped are specified as a set of fieldnames.
     *
     * The attributes that uniquely identify this document will actually be composed of multiple sets of attributes
     * where the grouping context is consistent within each set.  Note that fields with multiple of the same grouping
     * context and value will be collapsed into one distinct grouping context and value.  Fields with multiple of the
     * same grouping and different values will be considered one value consisting of an ordered value list.
     *
     * Example:
     *    Document:
     *       field1.a.b.0 = 1
     *       field2.a.b.0 = 2
     *       field1.a.b.1 = 3
     *       field3.c.d.0 = 10
     *       field3 = 11
     *       field3 = 12
     *       field4 = 100
     *       ...
     *    unique fields = field1, field2, field3, field4
     *    Resulting groups:
     *       field1 = 1, field2 = 2, field3 = 10, field4 = 100
     *       field1 = 1, field2 = 2, field3 = 11/12, field4 = 100
     *       field1 = 3, field2 = N/A, field3 = 10, field4 = 100
     *       field1 = 3, field2 = N/A, field3 = 11/12, field4 = 100
     *
     */
    private Set<Multimap<String, String>> getFieldSets(Document document) {
        Map<String, Multimap<String, String>> mapGroupingContextToField = new HashMap<>();
        for (String documentField : document.getDictionary().keySet()) {
            String field = getUniqueField(documentField);
            if (field != null) {
                String groupingContext = getGroupingContext(documentField);
                Set<String> values = getValues(document.get(documentField));
                Multimap<String, String> groupedValues = mapGroupingContextToField.get(groupingContext);
                if (groupedValues == null) {
                    groupedValues = HashMultimap.create();
                    mapGroupingContextToField.put(groupingContext, groupedValues);
                }
                groupedValues.putAll(field, values);
            }
        }

        // pull out the fields that had no grouping context
        Multimap<String, String> noGroupingContextSet = mapGroupingContextToField.remove("");

        // combine grouped sets that are mutually exclusive
        Set<Multimap<String, String>> sets = new HashSet<>();
        for (String groupingContext : new HashSet<>(mapGroupingContextToField.keySet())) {
            Multimap<String, String> group = mapGroupingContextToField.remove(groupingContext);
            boolean combined = false;
            for (Multimap<String, String> otherGroup : mapGroupingContextToField.values()) {
                if (!intersects(group.keySet(), otherGroup.keySet())) {
                    otherGroup.putAll(group);
                    combined = true;
                }
            }
            // if this group did not get combined with any other, then it is a final set
            if (!combined) {
                sets.add(group);
            }
        }

        // now for each field with an empty grouping context, distribute it across all of the other groups
        if (noGroupingContextSet != null && !noGroupingContextSet.isEmpty()) {
            for (String field : noGroupingContextSet.keySet()) {
                for (Multimap<String, String> group : mapGroupingContextToField.values()) {
                    if (!group.containsKey(field)) {
                        group.putAll(field, noGroupingContextSet.get(field));
                    }
                }
            }

        }

        return sets;
    }

    private boolean intersects(Set<String> set1, Set<String> set2) {
        for (String a : set1) {
            if (set2.contains(a)) {
                return true;
            }
        }
        return false;
    }

    private Set<String> getValues(Attribute<?> attr) {
        Set<String> values = new HashSet<>();
        if (attr instanceof Attributes) {
            for (Attribute<?> child : ((Attributes)attr).getAttributes()) {
                values.addAll(getValues(child));
            }
        } else {
            values.add(String.valueOf(attr.getData()));
        }
        return values;
    }

    private String getBaseFieldname(String documentField) {
        int index = documentField.indexOf('.');
        if (index < 0) {
            return documentField;
        } else {
            return documentField.substring(0, index);
        }
    }

    private String getGroupingContext(String documentField) {
        int index = documentField.indexOf('.');
        if (index < 0) {
            return "";
        } else {
            return documentField.substring(index+1);
        }
    }

    private String getUniqueField(String documentField) {
        String baseDocumentField = getBaseFieldname(documentField);
        for (String field : fields) {
            if (isMatchingField(baseDocumentField, field)) {
                return field;
            }
        }
        return null;
    }

    private boolean isMatchingField(String baseDocumentField, String field) {
        if (field.equals(baseDocumentField)) {
            return true;
        }
        if (modelMapping != null && modelMapping.get(field).contains(baseDocumentField)) {
            return true;
        }
        return false;
    }

    
    private boolean isDuplicate(Document document) throws IOException {
        byte[] bytes = getBytes(document);
        ByteSequence byteSeq = new ArrayByteSequence(bytes);
        if (bloom.mightContain(bytes)) {
            if (DEBUG && !seen.contains(byteSeq)) {
                throw new IllegalStateException("This event is 1 in 1Q!");
            } else {
                return true;
            }
        }
        bloom.put(bytes);
        if (DEBUG) {
            seen.add(byteSeq);
        }
        return false;
    }
    
    public static class ByteFunnel implements Funnel<byte[]>, Serializable {
        
        private static final long serialVersionUID = -2126172579955897986L;
        
        public ByteFunnel() {}
        
        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }
        
    }
    
}
