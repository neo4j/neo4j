package org.neo4j.onlinebackup;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.impl.transaction.XaDataSourceManager;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * Wraps an EmbeddedNeo to be used as data source and give access to other data
 * sources as well.
 */
public class EmbeddedNeoResource extends AbstractResource implements
    NeoResource
{
    protected final EmbeddedNeo neo;
    protected final XaDataSourceManager xaDsm;

    EmbeddedNeoResource( EmbeddedNeo neo )
    {
        super( neo.getConfig().getPersistenceModule().getPersistenceSource()
            .getXaDataSource() );
        this.neo = neo;
        this.xaDsm = neo.getConfig().getTxModule().getXaDataSourceManager();
    }

    public XaDataSourceResource getDataSource( String name )
    {
        XaDataSource ds = xaDsm.getXaDataSource( name );
        if ( ds == null )
        {
            return null;
        }
        return new XaDataSourceResource( ds );
    }

    public XaDataSourceResource getDataSource()
    {
        XaDataSource ds = neo.getConfig().getPersistenceModule()
            .getPersistenceSource().getXaDataSource();
        if ( ds == null )
        {
            return null;
        }
        return new XaDataSourceResource( ds );
    }

    @Override
    public void close()
    {
        neo.shutdown();
    }
}
