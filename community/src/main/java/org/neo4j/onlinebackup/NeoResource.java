package org.neo4j.onlinebackup;

/**
 * Wrap a XA resource together with additional methods to get this and other
 * data sources.
 */
public interface NeoResource extends Resource
{
    XaDataSourceResource getDataSource( String name );

    XaDataSourceResource getDataSource();

    String getName();
}
