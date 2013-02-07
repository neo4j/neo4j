/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
