package org.neo4j.helpers;

/**
 * Predicate useful for filtering.
 * 
 * @param <T> type of items
 */
public interface Predicate<T>
{
    /**
     * @return whether or not to accept the {@code item}, where {@code true}
     * means that the {@code item} is accepted and {@code false} means that
     * it's not (i.e. didn't pass the filter).
     */
    boolean accept( T item );
}
