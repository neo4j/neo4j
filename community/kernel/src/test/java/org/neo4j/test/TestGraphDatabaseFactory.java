/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.AbstractLogService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

/**
 * Test factory for graph databases
 */
public class TestGraphDatabaseFactory extends GraphDatabaseFactory
{
    public TestGraphDatabaseFactory()
    {
        super( new TestGraphDatabaseFactoryState() );
        setUserLogProvider( NullLogProvider.getInstance() );
    }

    public GraphDatabaseService newImpermanentDatabase()
    {
        return newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    public GraphDatabaseService newImpermanentDatabase( File storeDir )
    {
        return newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
    }

    public GraphDatabaseService newImpermanentDatabase( Map<Setting<?>, String> config )
    {
        GraphDatabaseBuilder builder = newImpermanentDatabaseBuilder();
        for ( Map.Entry<Setting<?>, String> entry : config.entrySet() )
        {
            builder.setConfig( entry.getKey(), entry.getValue() );
        }
        return builder.newGraphDatabase();
    }

    public GraphDatabaseBuilder newImpermanentDatabaseBuilder()
    {
        return newImpermanentDatabaseBuilder( ImpermanentGraphDatabase.PATH );
    }

    @Override
    protected void configure( GraphDatabaseBuilder builder )
    {
        super.configure( builder );
        // Reduce the default page cache memory size to 8 mega-bytes for test databases.
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
    }

    @Override
    protected TestGraphDatabaseFactoryState getCurrentState()
    {
        return (TestGraphDatabaseFactoryState) super.getCurrentState();
    }

    @Override
    protected TestGraphDatabaseFactoryState getStateCopy()
    {
        return new TestGraphDatabaseFactoryState( getCurrentState() );
    }

    public FileSystemAbstraction getFileSystem()
    {
        return getCurrentState().getFileSystem();
    }

    public TestGraphDatabaseFactory setFileSystem( FileSystemAbstraction fileSystem )
    {
        getCurrentState().setFileSystem( fileSystem );
        return this;
    }

    public GraphDatabaseFactory setMonitors( Monitors monitors )
    {
        getCurrentState().setMonitors( monitors );
        return this;
    }

    @Override
    public TestGraphDatabaseFactory setUserLogProvider( LogProvider logProvider )
    {
        return (TestGraphDatabaseFactory) super.setUserLogProvider( logProvider );
    }

    public TestGraphDatabaseFactory setInternalLogProvider( LogProvider logProvider )
    {
        getCurrentState().setInternalLogProvider( logProvider );
        return this;
    }

    @Override
    public TestGraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        return (TestGraphDatabaseFactory) super.addKernelExtensions( newKernelExtensions );
    }

    @Override
    public TestGraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        return (TestGraphDatabaseFactory) super.addKernelExtension( newKernelExtension );
    }

    @Override
    public TestGraphDatabaseFactory addURLAccessRule( String protocol, URLAccessRule rule )
    {
        return (TestGraphDatabaseFactory) super.addURLAccessRule( protocol, rule );
    }

    public GraphDatabaseBuilder newImpermanentDatabaseBuilder( final File storeDir )
    {
        final TestGraphDatabaseFactoryState state = getStateCopy();
        GraphDatabaseBuilder.DatabaseCreator creator =
                createImpermanentDatabaseCreator( storeDir, state );
        TestGraphDatabaseBuilder builder = createImpermanentGraphDatabaseBuilder( creator );
        configure( builder );
        return builder;
    }

    protected TestGraphDatabaseBuilder createImpermanentGraphDatabaseBuilder(
            GraphDatabaseBuilder.DatabaseCreator creator )
    {
        return new TestGraphDatabaseBuilder( creator );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
                                                                                     final TestGraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            @SuppressWarnings("deprecation")
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                return new CommunityFacadeFactory()
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                            Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                            OperationalMode operationalMode )
                    {
                        return new ImpermanentGraphDatabase.ImpermanentPlatformModule( storeDir, params, dependencies, graphDatabaseFacade )
                        {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction()
                            {
                                FileSystemAbstraction fs = state.getFileSystem();
                                if ( fs != null )
                                {
                                    return fs;
                                }
                                else
                                {
                                    return super.createFileSystemAbstraction();
                                }
                            }

                            @Override
                            protected LogService createLogService(LogProvider logProvider)
                            {
                                final LogProvider internalLogProvider = state.getInternalLogProvider();
                                if ( internalLogProvider == null )
                                {
                                    return super.createLogService(logProvider);
                                }

                                final LogProvider userLogProvider = state.databaseDependencies().userLogProvider();
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
                }.newFacade( storeDir, config, GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }
}
