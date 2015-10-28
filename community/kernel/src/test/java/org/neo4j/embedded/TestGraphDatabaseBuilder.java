/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.embedded;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.AbstractLogService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

/**
 * A builder for a TestGraphDatabase.
 *
 * @param <BUILDER> The concrete type of the builder
 * @param <GRAPHDB> The concrete type of the GraphDatabase being built
 */
abstract class TestGraphDatabaseBuilder<BUILDER extends TestGraphDatabaseBuilder<BUILDER,GRAPHDB>, GRAPHDB extends TestGraphDatabase>
        extends GraphDatabaseBuilder<BUILDER,GRAPHDB>
{
    protected FileSystemAbstraction fs;
    protected LogProvider internalLogProvider;
    protected Monitors monitors;
    protected IdGeneratorFactory idFactory;

    TestGraphDatabaseBuilder()
    {
        withParam( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
    }

    protected abstract BUILDER self();

    public BUILDER withFileSystem( FileSystemAbstraction fs )
    {
        this.fs = fs;
        return self();
    }

    public BUILDER withInternalLogProvider( LogProvider logProvider )
    {
        this.internalLogProvider = logProvider;
        return self();
    }

    public BUILDER withMonitors( Monitors monitors )
    {
        this.monitors = monitors;
        return self();
    }

    public BUILDER addKernelExtension( KernelExtensionFactory<?> kernelExtension )
    {
        this.kernelExtensions.add( kernelExtension );
        return self();
    }

    public BUILDER addKernelExtensions( Collection<KernelExtensionFactory<?>> kernelExtensions )
    {
        this.kernelExtensions.addAll( kernelExtensions );
        return self();
    }

    public BUILDER withKernelExtensions( Collection<KernelExtensionFactory<?>> kernelExtensions )
    {
        this.kernelExtensions.clear();
        return addKernelExtensions( kernelExtensions );
    }

    public BUILDER withIdGeneratorFactory( IdGeneratorFactory idFactory )
    {
        this.idFactory = idFactory;
        return self();
    }

    @Override
    protected GraphDatabaseDependencies createDependencies()
    {
        return super.createDependencies().monitors( monitors );
    }

    @Override
    protected GraphDatabaseFacadeFactory createFacadeFactory()
    {
        // TODO: replace overriding with dependency injection
        return new CommunityFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String,String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
            {
                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        return (fs != null) ? fs : super.createFileSystemAbstraction();
                    }

                    @Override
                    protected LogService createLogService( final LogProvider userLogProvider )
                    {
                        if ( internalLogProvider == null )
                        {
                            return super.createLogService( userLogProvider );
                        }

                        return new AbstractLogService()
                        {
                            @Override
                            public LogProvider getUserLogProvider()
                            {
                                return userLogProvider;
                            }

                            @Override
                            public LogProvider getInternalLogProvider()
                            {
                                return internalLogProvider;
                            }
                        };
                    }
                };
            }

            @Override
            protected EditionModule createEdition( PlatformModule platformModule )
            {
                return new CommunityEditionModule( platformModule )
                {
                    @Override
                    protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs )
                    {
                        return (idFactory != null) ? idFactory : super.createIdGeneratorFactory( fs );
                    }
                };
            }
        };
    }
}
