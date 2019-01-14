/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.ephemeral;


/**
 * Test factory for graph databases.
 * Please be aware that since it's a database it will close filesystem as part of its lifecycle.
 * If you expect your file system to be open after database is closed, use {@link UncloseableDelegatingFileSystemAbstraction}
 */
public class TestGraphDatabaseFactory extends GraphDatabaseFactory
{
    public TestGraphDatabaseFactory()
    {
        this( NullLogProvider.getInstance() );
    }

    public TestGraphDatabaseFactory( LogProvider logProvider )
    {
        super( new TestGraphDatabaseFactoryState() );
        setUserLogProvider( logProvider );
    }

    public GraphDatabaseService newImpermanentDatabase()
    {
        return newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    public GraphDatabaseService newImpermanentDatabase( File storeDir )
    {
        return newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
    }

    public GraphDatabaseService newImpermanentDatabase( Map<Setting<?>,String> config )
    {
        GraphDatabaseBuilder builder = newImpermanentDatabaseBuilder();
        setConfig( config, builder );
        return builder.newGraphDatabase();
    }

    public GraphDatabaseService newImpermanentDatabase( File storeDir , Map<Setting<?>,String> config )
    {
        GraphDatabaseBuilder builder = newImpermanentDatabaseBuilder(storeDir);
        setConfig( config, builder );
        return builder.newGraphDatabase();
    }

    private void setConfig( Map<Setting<?>,String> config, GraphDatabaseBuilder builder )
    {
        for ( Map.Entry<Setting<?>,String> entry : config.entrySet() )
        {
            Setting<?> key = entry.getKey();
            String value = entry.getValue();
            builder.setConfig( key, value );
        }
    }

    public GraphDatabaseBuilder newImpermanentDatabaseBuilder()
    {
        return newImpermanentDatabaseBuilder( ImpermanentGraphDatabase.PATH );
    }

    @Override
    protected void configure( GraphDatabaseBuilder builder )
    {
        // Reduce the default page cache memory size to 8 mega-bytes for test databases.
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        builder.setConfig( GraphDatabaseSettings.shutdown_transaction_end_timeout, "1s" );
        builder.setConfig( new BoltConnector( "bolt" ).type, BOLT.name() );
        builder.setConfig( new BoltConnector( "bolt" ).enabled, "false" );
    }

    private void configure( GraphDatabaseBuilder builder, File storeDir )
    {
        configure( builder );
        builder.setConfig( GraphDatabaseSettings.logs_directory, new File( storeDir, "logs" ).getAbsolutePath() );
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

    @Override
    public TestGraphDatabaseFactory setMonitors( Monitors monitors )
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

    public TestGraphDatabaseFactory setClock( SystemNanoClock clock )
    {
        getCurrentState().setClock( clock );
        return this;
    }

    public TestGraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().addKernelExtensions( newKernelExtensions );
        return this;
    }

    public TestGraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        return addKernelExtensions( Collections.singletonList( newKernelExtension ) );
    }

    public TestGraphDatabaseFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().setKernelExtensions( newKernelExtensions );
        return this;
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
        configure( builder, storeDir );
        return builder;
    }

    private TestGraphDatabaseBuilder createImpermanentGraphDatabaseBuilder(
            GraphDatabaseBuilder.DatabaseCreator creator )
    {
        return new TestGraphDatabaseBuilder( creator );
    }

    @Override
    protected GraphDatabaseService newEmbeddedDatabase( File storeDir, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return new TestGraphDatabaseFacadeFactory( getCurrentState() ).newFacade( storeDir, config,
                GraphDatabaseDependencies.newDependencies( dependencies ) );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String,String> config )
            {
                return newDatabase( Config.defaults( config ) );
            }

            @Override
            public GraphDatabaseService newDatabase( @Nonnull Config config )
            {
                return new TestGraphDatabaseFacadeFactory( state, true ).newFacade( storeDir, config,
                        GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }

    public static class TestGraphDatabaseFacadeFactory extends GraphDatabaseFacadeFactory
    {
        private final TestGraphDatabaseFactoryState state;
        private final boolean impermanent;

        protected TestGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state, boolean impermanent )
        {
            this( state, impermanent, DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        }

        protected TestGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state, boolean impermanent,
                DatabaseInfo databaseInfo, Function<PlatformModule,EditionModule> editionFactory )
        {
            super( databaseInfo, editionFactory );
            this.state = state;
            this.impermanent = impermanent;
        }

        TestGraphDatabaseFacadeFactory( TestGraphDatabaseFactoryState state )
        {
            this( state, false );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                GraphDatabaseFacade graphDatabaseFacade )
        {
            config.augment( GraphDatabaseSettings.database_path, storeDir.getAbsolutePath() );
            if ( impermanent )
            {
                config.augment( ephemeral, TRUE );
                return new ImpermanentTestDatabasePlatformModule( storeDir, config, dependencies, graphDatabaseFacade,
                        this.databaseInfo );
            }
            else
            {
                return new TestDatabasePlatformModule( storeDir, config, dependencies, graphDatabaseFacade,
                        this.databaseInfo );
            }
        }

        class TestDatabasePlatformModule extends PlatformModule
        {

            TestDatabasePlatformModule( File storeDir, Config config, Dependencies dependencies,
                    GraphDatabaseFacade graphDatabaseFacade, DatabaseInfo databaseInfo )
            {
                super( storeDir, config, databaseInfo, dependencies,
                        graphDatabaseFacade );
            }

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
                    return createNewFileSystem();
                }
            }

            protected FileSystemAbstraction createNewFileSystem()
            {
                return super.createFileSystemAbstraction();
            }

            @Override
            protected LogService createLogService( LogProvider userLogProvider )
            {
                LogProvider internalLogProvider = state.getInternalLogProvider();
                if ( internalLogProvider == null )
                {
                    if ( !impermanent )
                    {
                        return super.createLogService( userLogProvider );
                    }
                    internalLogProvider = NullLogProvider.getInstance();
                }
                return new SimpleLogService( userLogProvider, internalLogProvider );
            }

            @Override
            protected SystemNanoClock createClock()
            {
                SystemNanoClock clock = state.clock();
                return clock != null ? clock : super.createClock();
            }
        }

        private class ImpermanentTestDatabasePlatformModule extends TestDatabasePlatformModule
        {

            ImpermanentTestDatabasePlatformModule( File storeDir, Config config,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade, DatabaseInfo databaseInfo )
            {
                super( storeDir, config, dependencies, graphDatabaseFacade, databaseInfo );
            }

            @Override
            protected FileSystemAbstraction createNewFileSystem()
            {
                return new EphemeralFileSystemAbstraction();
            }

            @Override
            protected StoreLocker createStoreLocker()
            {
                return new StoreLocker( fileSystem, storeDir );
            }
        }
    }
}
