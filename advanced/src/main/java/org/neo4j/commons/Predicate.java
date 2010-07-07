package org.neo4j.commons;

/**
 * Predicate useful for filtering.
 * 
 * @param <T> type of items
 */
public interface Predicate<T>
{
    boolean accept( T item );
}
