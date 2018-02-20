package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.storageengine.api.EntityType;

public interface FulltextAccessor
{
    Stream<String> propertyKeyStrings( IndexDescriptor descriptor );

    IndexDescriptor indexDescriptorFor( String name, EntityType type, String[] entityTokens, String... properties );

    PrimitiveLongIterator query( String indexName, String queryString ) throws IOException;
}
