package org.neo4j.graphdb.index;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Extends the {@link Index} interface with additional get/query methods which
 * are specific to {@link Relationship}s. Each of {@link #get(String, Object)},
 * {@link #query(String, Object)} and {@link #query(Object)} have an additional
 * method which allows filtering on start/end node of the relationships.
 * 
 * @author Mattias Persson
 */
public interface RelationshipIndex extends Index<Relationship>
{
    /**
     * Returns exact matches from this index, given the key/value pair.
     * Matches will be for key/value pairs just as they were added by the
     * {@link #add(PropertyContainer, String, Object)} method. 
     * 
     * @param key the key in the key/value pair to match.
     * @param value the value in the key/value pair to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     * that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with
     * that given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
            Node endNodeOrNull );
    
    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     * 
     * @param key the key in this query.
     * @param queryOrQueryObject the query for the {@code key} to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     * that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with
     * that given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
            Node startNodeOrNull, Node endNodeOrNull );
    
    /**
     * Returns matches from this index based on the supplied query object,
     * which can be a query string or an implementation-specific query object.
     * 
     * @param queryOrQueryObject the query to match.
     * @param startNodeOrNull filter so that only {@link Relationship}s with
     * that given start node will be returned.
     * @param endNodeOrNull filter so that only {@link Relationship}s with
     * that given end node will be returned.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull,
            Node endNodeOrNull );
}
