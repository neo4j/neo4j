package org.neo4j.storageengine.api;

import java.util.function.LongPredicate;

public interface StorageEntityCursor extends AutoCloseable
{
    boolean next( LongPredicate filter );

    boolean hasProperties();

    long propertiesReference();

    void single( long reference );

    @Override
    void close();
}
