package datawave.query.postprocessing.tf;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Iterator that fetches all keys defined by the seek range. Used by the {@link TermFrequencyHitFunction} to determine if a field value pair is present in a
 * document. In the TLD case this iterator will also fetch all child uids associatd with the TLD.
 */
public class DelayedFieldIndexIterator extends WrappingIterator {
    
    private static final Logger log = Logger.getLogger(DelayedFieldIndexIterator.class);
    
    private Key tk = null;
    private Value tv = null;
    
    // The initial search range
    private Range range;
    private SortedKeyValueIterator<Key,Value> source;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> seekCFs, boolean inclusive) throws IOException {
        this.range = new Range(range);
        this.tk = null;
        this.tv = null;
        
        source.seek(this.range, seekCFs, true);
        findTop();
    }
    
    @Override
    public void next() throws IOException {
        this.tk = null;
        this.tv = null;
        if (source.hasTop()) {
            source.next();
            findTop();
        }
    }
    
    private void findTop() {
        this.tk = null;
        this.tv = null;
        
        Key key;
        while (true) {
            
            // If the source iterator is exhausted we're done
            if (!source.hasTop()) {
                break;
            }
            
            key = source.getTopKey();
            
            // If the source iterator returned keys outside our range, we're done.
            if (!range.contains(key)) {
                break;
            }
            
            this.tk = key;
            this.tv = source.getTopValue();
            break;
        }
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DelayedFieldIndexIterator iter = new DelayedFieldIndexIterator();
        iter.source = source.deepCopy(env);
        return iter;
    }
    
    @Override
    public void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> getSource() {
        return this.source;
    }
    
    @Override
    public Key getTopKey() {
        return this.tk;
    }
    
    @Override
    public Value getTopValue() {
        return this.tv;
    }
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
}
