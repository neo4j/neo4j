package org.neo4j.kernel.api.impl.insight;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public interface InsightIndexReader extends AutoCloseable
{
    PrimitiveLongIterator query( String... query );
}
