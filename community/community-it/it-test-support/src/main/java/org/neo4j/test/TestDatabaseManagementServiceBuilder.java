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
import java.util.function.Predicate;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.factory.DatabaseManagementServiceInternalBuilder;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.connectors.Connector.ConnectorType.BOLT;

/**
 * Test factory for graph databases.
 * Please be aware that since it's a database it will close filesystem as part of its lifecycle.
 * If you expect your file system to be open after database is closed, use {@link UncloseableDelegatingFileSystemAbstraction}
 */
public class TestDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
{
    private static final File EPHEMERAL_PATH = new File( "target/test data/" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    public static final Predicate<ExtensionFactory<?>> INDEX_PROVIDERS_FILTER = extension -> extension instanceof AbstractIndexProviderFactory;

    public TestDatabaseManagementServiceBuilder()
    {
        this( NullLogProvider.getInstance() );
    }

    public TestDatabaseManagementServiceBuilder( LogProvider logProvider )
    {
        super( new TestGraphDatabaseFactoryState() );
        setUserLogProvider( logProvider );
    }

    public DatabaseManagementService newImpermanentService()
    {
        DatabaseManagementServiceInternalBuilder databaseBuilder = newImpermanentDatabaseBuilder();
        return databaseBuilder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( File storeDir )
    {
        DatabaseManagementServiceInternalBuilder databaseBuilder = newImpermanentDatabaseBuilder( storeDir );
        return databaseBuilder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( Map<Setting<?>,String> config )
    {
        DatabaseManagementServiceInternalBuilder builder = newImpermanentDatabaseBuilder();
        setConfig( config, builder );
        return builder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( File storeDir , Map<Setting<?>,String> config )
    {
        DatabaseManagementServiceInternalBuilder builder = newImpermanentDatabaseBuilder(storeDir);
        setConfig( config, builder );
        return builder.newDatabaseManagementService();
    }

    public DatabaseManagementServiceInternalBuilder newImpermanentDatabaseBuilder()
    {
        return newImpermanentDatabaseBuilder( EPHEMERAL_PATH );
    }

    @Override
    protected void configure( DatabaseManagementServiceInternalBuilder builder )
    {
        // Reduce the default page cache memory size to 8 mega-bytes for test databases.
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        builder.setConfig( new BoltConnector( "bolt" ).type, BOLT.name() );
        builder.setConfig( new BoltConnector( "bolt" ).enabled, "false" );
    }

    private void configure( DatabaseManagementServiceInternalBuilder builder, File storeDir )
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

    public TestDatabaseManagementServiceBuilder setFileSystem( FileSystemAbstraction fileSystem )
    {
        getCurrentState().setFileSystem( fileSystem );
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        getCurrentState().setMonitors( monitors );
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder setUserLogProvider( LogProvider logProvider )
    {
        return (TestDatabaseManagementServiceBuilder) super.setUserLogProvider( logProvider );
    }

    public TestDatabaseManagementServiceBuilder setInternalLogProvider( LogProvider logProvider )
    {
        getCurrentState().setInternalLogProvider( logProvider );
        return this;
    }

    public TestDatabaseManagementServiceBuilder setClock( SystemNanoClock clock )
    {
        getCurrentState().setClock( clock );
        return this;
    }

    private TestDatabaseManagementServiceBuilder addExtensions( Iterable<ExtensionFactory<?>> extensions )
    {
        getCurrentState().addExtensions( extensions );
        return this;
    }

    public TestDatabaseManagementServiceBuilder addExtension( ExtensionFactory<?> extension )
    {
        return addExtensions( Collections.singletonList( extension ) );
    }

    public TestDatabaseManagementServiceBuilder setExtensions( Iterable<ExtensionFactory<?>> extensions )
    {
        getCurrentState().setExtensions( extensions );
        return this;
    }

    public TestDatabaseManagementServiceBuilder removeExtensions( Predicate<ExtensionFactory<?>> filter )
    {
        getCurrentState().removeExtensions( filter );
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        return (TestDatabaseManagementServiceBuilder) super.addURLAccessRule( protocol, rule );
    }

    public DatabaseManagementServiceInternalBuilder newImpermanentDatabaseBuilder( final File storeDir )
    {
        final TestGraphDatabaseFactoryState state = getStateCopy();
        DatabaseManagementServiceInternalBuilder.DatabaseCreator creator =
                createImpermanentDatabaseCreator( storeDir, state );
        DatabaseManagementServiceInternalBuilder
                builder = new DatabaseManagementServiceInternalBuilder( creator ).setConfig( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        configure( builder, storeDir );
        return builder;
    }

    @Override
    protected DatabaseManagementService newEmbeddedDatabase( File storeDir, Config config,
            ExternalDependencies dependencies )
    {
        return new TestDatabaseManagementServiceFactory( getCurrentState() ).newFacade( storeDir, config,
                GraphDatabaseDependencies.newDependencies( dependencies ) );
    }

    protected DatabaseManagementServiceInternalBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return config -> new TestDatabaseManagementServiceFactory( state, true ).newFacade( storeDir, config,
                GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
    }

    private static void setConfig( Map<Setting<?>,String> config, DatabaseManagementServiceInternalBuilder builder )
    {
        for ( Map.Entry<Setting<?>,String> entry : config.entrySet() )
        {
            Setting<?> key = entry.getKey();
            String value = entry.getValue();
            builder.setConfig( key, value );
        }
    }
}
