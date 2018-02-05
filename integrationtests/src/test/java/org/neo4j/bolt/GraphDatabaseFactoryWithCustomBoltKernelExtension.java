/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toList;
import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;

public class GraphDatabaseFactoryWithCustomBoltKernelExtension extends GraphDatabaseFactory
{
    private final BoltKernelExtension customExtension;

    public GraphDatabaseFactoryWithCustomBoltKernelExtension( BoltKernelExtension customExtension )
    {
        this.customExtension = customExtension;
    }

    @Override
    protected GraphDatabaseService newDatabase( File storeDir, Config config, Dependencies dependencies )
    {
        GraphDatabaseFacadeFactory factory = new CustomBoltKernelExtensionFacadeFactory( customExtension );
        return factory.newFacade( storeDir, config, dependencies );
    }

    private static class CustomBoltKernelExtensionFacadeFactory extends GraphDatabaseFacadeFactory
    {
        final BoltKernelExtension customExtension;

        CustomBoltKernelExtensionFacadeFactory( BoltKernelExtension customExtension )
        {
            super( DatabaseInfo.ENTERPRISE, EnterpriseEditionModule::new );
            this.customExtension = customExtension;
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config,
                Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
        {
            Dependencies newDependencies = new CustomBoltKernelExtensionDependencies( customExtension, dependencies );
            return new PlatformModule( storeDir, config, databaseInfo, newDependencies, graphDatabaseFacade );
        }
    }

    private static class CustomBoltKernelExtensionDependencies implements Dependencies
    {
        final BoltKernelExtension customExtension;
        final Dependencies delegate;

        CustomBoltKernelExtensionDependencies( BoltKernelExtension customExtension, Dependencies delegate )
        {
            this.customExtension = customExtension;
            this.delegate = delegate;
        }

        @Override
        public Monitors monitors()
        {
            return delegate.monitors();
        }

        @Override
        public LogProvider userLogProvider()
        {
            return delegate.userLogProvider();
        }

        @Override
        public Iterable<Class<?>> settingsClasses()
        {
            return delegate.settingsClasses();
        }

        @Override
        public Iterable<KernelExtensionFactory<?>> kernelExtensions()
        {
            return Iterables.stream( delegate.kernelExtensions() )
                    .map( this::replaceBoltKernelExtensionFactory )
                    .collect( toList() );
        }

        @Override
        public Map<String,URLAccessRule> urlAccessRules()
        {
            return delegate.urlAccessRules();
        }

        @Override
        public Iterable<QueryEngineProvider> executionEngines()
        {
            return delegate.executionEngines();
        }

        KernelExtensionFactory<?> replaceBoltKernelExtensionFactory( KernelExtensionFactory<?> factory )
        {
            if ( factory instanceof BoltKernelExtension )
            {
                return new CustomBoltKernelExtension( customExtension );
            }
            return factory;
        }
    }

    /**
     * Each kernel extension factory is expected to extend {@link KernelExtensionFactory} and have some dependencies
     * as it's type parameter. That is why we can't use given custom extension as is, it can extend a real
     * {@link BoltKernelExtension}. So this wrapper delegates to the given extension and has same superclass as the
     * real {@link BoltKernelExtension}.
     *
     * @see KernelExtensions#getKernelExtensionDependencies(KernelExtensionFactory)
     */
    private static class CustomBoltKernelExtension extends KernelExtensionFactory<BoltKernelExtension.Dependencies>
    {
        final BoltKernelExtension customExtension;

        CustomBoltKernelExtension( BoltKernelExtension customExtension )
        {
            super( "custom-bolt-server" );
            this.customExtension = customExtension;
        }

        @Override
        public Lifecycle newInstance( KernelContext context, BoltKernelExtension.Dependencies dependencies )
        {
            return customExtension.newInstance( context, dependencies );
        }
    }
}
