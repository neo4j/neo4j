package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

/**
 * This is only for backwards compatibility with 1.8. Will be removed in the future. The right way to get this
 * is to instantiate through GraphDatabaseFactory.
 */
@Deprecated
public class HighlyAvailableGraphDatabase
    extends org.neo4j.kernel.ha.HighlyAvailableGraphDatabase
{
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config)
    {
        this(storeDir, config,
                Service.load( IndexProvider.class ),
                Iterables.<KernelExtensionFactory<?>, KernelExtensionFactory>cast(Service.load( KernelExtensionFactory.class )),
                Service.load( CacheProvider.class ),
                Service.load( TransactionInterceptorProvider.class ));
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> params, Iterable<IndexProvider> indexProviders, Iterable<KernelExtensionFactory<?>> kernelExtensions, Iterable<CacheProvider> cacheProviders, Iterable<TransactionInterceptorProvider> txInterceptorProviders )
    {
        super( storeDir, params, indexProviders, kernelExtensions, cacheProviders, txInterceptorProviders );
    }

    @Deprecated
    public void pullUpdates()
    {
        dependencyResolver.resolveDependency( UpdatePuller.class ).pullUpdates();
    }
}
