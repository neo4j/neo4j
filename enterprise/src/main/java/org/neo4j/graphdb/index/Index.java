package org.neo4j.graphdb.index;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * An index to store key/value pairs for fast lookup and querying. Indexes
 * participates in transactions so rolling back and committing works the same
 * way as usual in Neo4j.
 * 
 * @author Mattias Persson
 *
 * @param <T> The type of items this index manages. It may be either
 * {@link Node}s or {@link Relationship}s.
 */
public interface Index<T extends PropertyContainer>
{
    /**
     * Adds a key/value pair for {@code entity} to the index. If that key/value
     * pair for the entity is already in the index it's up to the
     * implementation to make it so that such an operation is idempotent.
     * 
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to associate the key/value pair with.
     * @param key the key in the key/value pair to associate with the entity.
     * @param value the value in the key/value pair to associate with the
     * entity.
     */
    void add( T entity, String key, Object value );
    
    /**
     * Removes a key/value pair for {@code entity} from the index. If that
     * key/value pair isn't associated with {@code entity} in this index this
     * operation doesn't do anything.
     * 
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to dissociate the key/value pair from.
     * @param key the key in the key/value pair to dissociate from the entity.
     * @param value the value in the key/value pair to dissociate from the
     * entity.
     */
    void remove( T entity, String key, Object value );
    
    /**
     * Clears the index. After this nothing will be returned in any query
     * until key/value pairs are added again.
     */
    void delete();
    
    /**
     * Returns exact matches from this index, given the key/value pair.
     * Matches will be for key/value pairs just as they were added by the
     * {@link #add(PropertyContainer, String, Object)} method. 
     * 
     * @param key the key in the key/value pair to match.
     * @param value the value in the key/value pair to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<T> get( String key, Object value );
    
    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     * 
     * @param key the key in this query.
     * @param queryOrQueryObject the query for the {@code key} to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<T> query( String key, Object queryOrQueryObject );
    
    /**
     * Returns matches from this index based on the supplied query object,
     * which can be a query string or an implementation-specific query object.
     * 
     * @param queryOrQueryObject the query to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<T> query( Object queryOrQueryObject );
}
