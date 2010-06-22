package org.neo4j.commons;

public interface Predicate<T>
{
    boolean accept( T item );
}
