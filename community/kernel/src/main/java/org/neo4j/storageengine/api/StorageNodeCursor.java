package org.neo4j.storageengine.api;

import java.util.function.LongPredicate;

public interface StorageNodeCursor
{
    void scan();

    void single( long reference );

    long nodeReference();

    long[] labels();

    boolean hasLabel( int label );

    boolean hasProperties();

    long relationshipGroupReference();

    long allRelationshipsReference();

    long propertiesReference();

    boolean next( LongPredicate filter );

    void setCurrent( long nodeReference );

    void close();

    boolean isClosed();

    boolean isDense();

    void reset();

    void release();
}
