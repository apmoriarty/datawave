package datawave.query.transformer;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections4.iterators.TransformIterator;

import java.util.Iterator;
import java.util.Map;

public class GroupingTransformIterator extends TransformIterator<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    GroupingTransform groupingTransform;

    public GroupingTransformIterator(Iterator<Map.Entry<Key,Document>> iterator, GroupingTransform groupingTransform) {
        super(iterator, o -> groupingTransform.apply(o));
        this.groupingTransform = groupingTransform;
    }
    @Override
    public boolean hasNext() {
        while (super.hasNext()) {
            getTransformer().transform(super.next());
        }
        return groupingTransform.getMultiset().size() > 0;
    }

    @Override
    public Map.Entry<Key,Document> next() {
        return groupingTransform.gather();
    }

}
