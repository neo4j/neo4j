package org.neo4j.kernel.spi.legacyindex;


import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

/**
 * Registry of currently active index implementations. Indexing extensions should register the implementation
 * here on startup, and unregister it on stop.
 * @deprecated removed in 4.0
 */
@Deprecated
public interface IndexProviders
{
    void registerIndexProvider( String name, IndexImplementation index );

    boolean unregisterIndexProvider( String name );
}