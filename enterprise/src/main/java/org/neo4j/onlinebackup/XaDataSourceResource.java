package org.neo4j.onlinebackup;

import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * Simple wrapper for XA data sources.
 */
public class XaDataSourceResource extends AbstractResource implements Resource
{
    public XaDataSourceResource( XaDataSource xaDs )
    {
        super( xaDs );
    }

    @Override
    public void close()
    {
        xaDs.close();
    }
}
