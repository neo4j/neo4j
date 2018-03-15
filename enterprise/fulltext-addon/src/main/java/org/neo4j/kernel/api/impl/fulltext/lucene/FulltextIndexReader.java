package org.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

public abstract class FulltextIndexReader implements IndexReader
{
    /**
     * Queires the fulltext index with the given lucene-syntax query
     * @param query the lucene query
     * @return A {@link ScoreEntityIterator} over the results
     */
    public abstract ScoreEntityIterator query( String query );

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        //TODO Maybe we need to count indexed nodes somewhere?
        return 0;
    }

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        return PrimitiveLongResourceCollections.emptyIterator();
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, IndexQuery... query ) throws IndexNotApplicableKernelException
    {
        // Do nothing, the fulltext index does not support normal index queries.
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }
}
