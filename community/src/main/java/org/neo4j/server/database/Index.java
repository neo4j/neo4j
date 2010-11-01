package org.neo4j.server.database;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

/**
 * An abstraction of an {@link IndexService} which can handle either
 * {@link Node}s or {@link Relationship}s.
 * 
 * This will probably be removed if/when {@link IndexService} supports
 * indexing of relationships.
 */
public interface Index<T extends PropertyContainer>
{
    boolean add( T object, String key, Object value );
    
    boolean remove( T object, String key, Object value );
    
    boolean contains( T object, String key, Object value );
    
    IndexHits<T> get( String key, Object value );
}
