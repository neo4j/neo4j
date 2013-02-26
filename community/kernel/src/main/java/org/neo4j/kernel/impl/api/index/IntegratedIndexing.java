package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class IntegratedIndexing extends LifecycleAdapter
{
    public static final IntegratedIndexing NO_INDEXING = new IntegratedIndexing( null, null, null );

    private final ExecutorService executor;
    private final XaDataSourceManager dataSourceManager;
    private final SchemaIndexProvider provider;

    private final LifeSupport life;
    private final AtomicReference<IndexingService> serviceReference;

    public IntegratedIndexing( ExecutorService executor,
                               XaDataSourceManager dataSourceManager,
                               SchemaIndexProvider provider )
    {
        this.executor = executor;
        this.dataSourceManager = dataSourceManager;
        this.provider = provider;

        this.life = new LifeSupport();
        this.serviceReference = new AtomicReference<IndexingService>( IndexingService.Adapter.EMPTY );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        dataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener.Adapter()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( ds.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
                {
                    NeoStore neoStore = ((NeoStoreXaDataSource)ds).getNeoStore();
                    IntegratedIndexingService service = new IntegratedIndexingService( executor, provider, neoStore );
                    serviceReference.set( service );
                    life.add( service );
                }
            }
        } );
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
        serviceReference.set( IndexingService.Adapter.EMPTY );
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    public IndexingService getService() {
        return serviceReference.get();
    }
}
