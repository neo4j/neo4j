package org.neo4j.graphdb.index;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

/**
 * The {@link BatchInserter} version of {@link Index}. Additions/updates to a
 * {@link BatchInserterIndex} doesn't necessarily gets added to the actual
 * index immediately, but are instead forced to be writted when the index is
 * shut down, {@link BatchInserterIndexProvider#shutdown()}.
 * 
 * To guarantee additions/updates are seen by {@link #updateOrAdd(long, Map)},
 * {@link #get(String, Object)}, {@link #query(String, Object)} and
 * {@link #query(Object)} a call to {@link #flush()} must be made prior to
 * calling such a method. This enables implementations more flexibility in
 * making for performance optimizations.
 */
public interface BatchInserterIndex
{
    /**
     * Adds key/value pairs for {@code entity} to the index. If there's a
     * previous index for {@code entity} it will co-exist with this new one.
     * This behaviour is because of performance reasons, to not being forced
     * to check if indexing for {@code entity} already exists or not. If you
     * need to update indexing for {@code entity} and it's ok with a slower
     * indexing process use {@link #addOrUpdate(long, Map)} instead.
     * 
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made.
     * 
     * @param entity the entity (i.e id of {@link Node} or
     * {@link Relationship}) to associate the key/value pairs with.
     * @param properties key/value pairs to index for {@code entity}.
     */
    void add( long entityId, Map<String, Object> properties );
    
    /**
     * Adds key/value pairs for {@code entity} to the index. If there's a
     * previous index for {@code entity} it will be overwritten. This
     * method can be considerably slower than {@link #add(long, Map)} because
     * it must check if there are properties already indexed for
     * {@code entity}. So if you know that there's no previous indexing for
     * {@code entity} use {@link #add(long, Map)} instead.
     * 
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     * 
     * @param entity the entity (i.e id of {@link Node} or
     * {@link Relationship}) to associate the key/value pairs with.
     * @param properties key/value pairs to index for {@code entity}.
     */
    void updateOrAdd( long entityId, Map<String, Object> properties );
    
    /**
     * Returns exact matches from this index, given the key/value pair.
     * Matches will be for key/value pairs just as they were added by the
     * {@link #add(long, Map)} or {@link #addOrUpdate(long, Map)} method. 
     * 
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     * 
     * @param key the key in the key/value pair to match.
     * @param value the value in the key/value pair to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Long> get( String key, Object value );
    
    /**
     * Returns matches from this index based on the supplied {@code key} and
     * query object, which can be a query string or an implementation-specific
     * query object.
     * 
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     * 
     * @param key the key in this query.
     * @param queryOrQueryObject the query for the {@code key} to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Long> query( String key, Object queryOrQueryObject );
    
    /**
     * Returns matches from this index based on the supplied query object,
     * which can be a query string or an implementation-specific query object.
     * 
     * Entries added to the index aren't necessarily written to the index and
     * to disk until {@link BatchInserterIndexProvider#shutdown()} has been
     * called. Entries added to the index isn't necessarily seen by other
     * methods: {@link #updateOrAdd(long, Map)}, {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} until a call
     * to {@link #flush()} has been made. So only entries added before the most
     * recent {@link #flush()} are guaranteed to be found by this method.
     * 
     * @param queryOrQueryObject the query to match.
     * @return the result wrapped in an {@link IndexHits} object. If the entire
     * result set isn't looped through, {@link IndexHits#close()} must be
     * called before disposing of the result.
     */
    IndexHits<Long> query( Object queryOrQueryObject );
    
    /**
     * Makes sure additions/updates can be seen by {@link #get(String, Object)},
     * {@link #query(String, Object)} and {@link #query(Object)} so that they
     * are guaranteed to return correct results. Also
     * {@link #updateOrAdd(long, Map)} will find previous indexing correctly
     * after a flush.
     */
    void flush();
}
