package datawave.query.postprocessing.tf;

import datawave.query.Constants;
import datawave.query.iterator.SortedListKeyValueIterator;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class DelayedFieldIndexIteratorTest {
    
    @Test
    public void test_eventQueryAgainstEvents() throws IOException {
        Key start = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0");
        Range range = new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
        
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0TEXT"));
        
        List<Key> expected = new LinkedList<>();
        expected.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0"));
        
        test(range, seekCFs, expected, createEventSource());
    }
    
    @Test
    public void test_eventQueryAgainstTld() throws IOException {
        Key start = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0");
        Range range = new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
        
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0TEXT"));
        
        List<Key> expected = new LinkedList<>();
        expected.add(start);
        
        test(range, seekCFs, expected, createTldSource());
    }
    
    @Test
    public void test_tldQueryAgainstEvents() throws IOException {
        Key start = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0");
        Key end = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0" + Constants.MAX_UNICODE_STRING);
        Range range = new Range(start, true, end, false);
        
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0TEXT"));
        
        List<Key> expected = new LinkedList<>();
        expected.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0"));
        
        test(range, seekCFs, expected, createEventSource());
    }
    
    @Test
    public void test_tldQueryAgainstTld() throws IOException {
        Key start = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0");
        Key end = new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0" + Constants.MAX_UNICODE_STRING);
        Range range = new Range(start, true, end, false);
        
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0TEXT"));
        
        List<Key> expected = new LinkedList<>();
        expected.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0"));
        expected.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0.1"));
        expected.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0.2"));
        
        test(range, seekCFs, expected, createTldSource());
    }
    
    private void test(Range range, Collection<ByteSequence> seekCFs, List<Key> expected, SortedKeyValueIterator<Key,Value> source) throws IOException {
        DelayedFieldIndexIterator iter = new DelayedFieldIndexIterator();
        iter.init(source, null, null);
        iter.seek(range, seekCFs, true);
        
        List<Key> scanned = new LinkedList<>();
        while (iter.hasTop()) {
            scanned.add(iter.getTopKey());
            iter.next();
        }
        
        assertEquals(expected, scanned);
    }
    
    private SortedKeyValueIterator<Key,Value> createEventSource() {
        TreeSet<Key> sorted = new TreeSet<>();
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0"));
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid1"));
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid2"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid0"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid1"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid2"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid0"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid1"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid2"));
        
        // Field Index source
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (Key k : sorted) {
            data.add(new AbstractMap.SimpleEntry<>(k, new Value()));
        }
        return new SortedListKeyValueIterator(data);
    }
    
    private SortedKeyValueIterator<Key,Value> createTldSource() {
        TreeSet<Key> sorted = new TreeSet<>();
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0"));
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0.1"));
        sorted.add(new Key("shard", "fi\0TEXT", "foo\0datatype\0uid0.2"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid1"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid1.1"));
        sorted.add(new Key("shard", "fi\0TEXT", "fox\0datatype\0uid1.2"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid2"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid2.1"));
        sorted.add(new Key("shard", "fi\0TEXT", "foxy\0datatype\0uid2.2"));
        
        // Field Index source
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (Key k : sorted) {
            data.add(new AbstractMap.SimpleEntry<>(k, new Value()));
        }
        return new SortedListKeyValueIterator(data);
    }
}
