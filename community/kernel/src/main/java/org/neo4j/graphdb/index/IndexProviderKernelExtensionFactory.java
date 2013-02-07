package org.neo4j.graphdb.index;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This is a temporary adapter for IndexProviders. To be removed after IndexProvider is removed.
 */
@Deprecated
public class IndexProviderKernelExtensionFactory
    extends KernelExtensionFactory<IndexProviderKernelExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        IndexProviders indexProviders();
        DependencyResolver resolver();
    }

    private IndexProvider indexProvider;

    public IndexProviderKernelExtensionFactory( IndexProvider indexProvider )
    {
        super(indexProvider.identifier());
        this.indexProvider = indexProvider;
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return new IndexProviderKernelExtension(indexProvider, dependencies.resolver(), dependencies.indexProviders());
    }

    private class IndexProviderKernelExtension extends LifecycleAdapter
    {
        private IndexProvider indexProvider;
        private DependencyResolver resolver;
        private IndexProviders indexProviders;
        private IndexImplementation indexImplementation;

        public IndexProviderKernelExtension( IndexProvider indexProvider, DependencyResolver resolver, IndexProviders
                indexProviders )
        {
            this.indexProvider = indexProvider;
            this.resolver = resolver;
            this.indexProviders = indexProviders;
        }

        @Override
        public void start() throws Throwable
        {
            indexImplementation = indexProvider.load( resolver );
            if (indexImplementation != null)
                indexProviders.registerIndexProvider( indexProvider.identifier(), indexImplementation );
        }

        @Override
        public void stop() throws Throwable
        {
            if (indexImplementation != null)
            {
                indexProviders.unregisterIndexProvider( indexProvider.identifier() );
                indexImplementation = null;
            }
        }
    }
}
