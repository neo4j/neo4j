package org.neo4j.index.impl.lucene;

import org.apache.lucene.document.Document;

interface EntityType
{
    Document newDocument( long entityId );
    
    Class<?> getType();
}
