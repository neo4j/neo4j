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

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
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

    protected FileSystemAbstraction fileSystem;
    protected LogProvider internalLogProvider;
    protected SystemNanoClock clock;

    public TestDatabaseManagementServiceBuilder()
    {
        this( NullLogProvider.getInstance() );
    }

    public TestDatabaseManagementServiceBuilder( LogProvider logProvider )
    {
        super();
        setUserLogProvider( logProvider );
    }

    public DatabaseManagementService newImpermanentService()
    {
        DatabaseManagementServiceBuilder databaseBuilder = newImpermanentDatabaseBuilder();
        return databaseBuilder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( File storeDir )
    {
        DatabaseManagementServiceBuilder databaseBuilder = newImpermanentDatabaseBuilder( storeDir );
        return databaseBuilder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( Map<Setting<?>,String> config )
    {
        DatabaseManagementServiceBuilder builder = newImpermanentDatabaseBuilder();
        setConfig( config, builder );
        return builder.newDatabaseManagementService();
    }

    public DatabaseManagementService newImpermanentService( File storeDir , Map<Setting<?>,String> config )
    {
        DatabaseManagementServiceBuilder builder = newImpermanentDatabaseBuilder(storeDir);
        setConfig( config, builder );
        return builder.newDatabaseManagementService();
    }

    public DatabaseManagementServiceBuilder newImpermanentDatabaseBuilder()
    {
        return newImpermanentDatabaseBuilder( EPHEMERAL_PATH );
    }

    @Override
    protected void configure( DatabaseManagementServiceBuilder builder )
    {
        // Reduce the default page cache memory size to 8 mega-bytes for test databases.
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        builder.setConfig( new BoltConnector( "bolt" ).type, BOLT.name() );
        builder.setConfig( new BoltConnector( "bolt" ).enabled, "false" );
    }

    private void configure( DatabaseManagementServiceBuilder builder, File storeDir )
    {
        configure( builder );
        builder.setConfig( GraphDatabaseSettings.logs_directory, new File( storeDir, "logs" ).getAbsolutePath() );
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public TestDatabaseManagementServiceBuilder setFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        return (TestDatabaseManagementServiceBuilder) super.setExternalDependencies( dependencies );
    }

    @Override
    public TestDatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        return (TestDatabaseManagementServiceBuilder) super.setMonitors( monitors );
    }

    @Override
    public TestDatabaseManagementServiceBuilder setUserLogProvider( LogProvider logProvider )
    {
        return (TestDatabaseManagementServiceBuilder) super.setUserLogProvider( logProvider );
    }

    public TestDatabaseManagementServiceBuilder setInternalLogProvider( LogProvider internalLogProvider )
    {
        this.internalLogProvider = internalLogProvider;
        return this;
    }

    public TestDatabaseManagementServiceBuilder setClock( SystemNanoClock clock )
    {
        this.clock = clock;
        return this;
    }

    private TestDatabaseManagementServiceBuilder addExtensions( Iterable<ExtensionFactory<?>> extensions )
    {
        for ( ExtensionFactory<?> extension : extensions )
        {
            this.extensions.add( extension );
        }
        return this;
    }

    public TestDatabaseManagementServiceBuilder addExtension( ExtensionFactory<?> extension )
    {
        return addExtensions( Collections.singletonList( extension ) );
    }

    public TestDatabaseManagementServiceBuilder setExtensions( Iterable<ExtensionFactory<?>> newExtensions )
    {
        extensions.clear();
        addExtensions( newExtensions );
        return this;
    }

    public TestDatabaseManagementServiceBuilder removeExtensions( Predicate<ExtensionFactory<?>> toRemove )
    {
        extensions.removeIf( toRemove );
        return this;
    }

    @Override
    public TestDatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        return (TestDatabaseManagementServiceBuilder) super.addURLAccessRule( protocol, rule );
    }

    public DatabaseManagementServiceBuilder newImpermanentDatabaseBuilder( final File storeDir )
    {
        creator = new EmbeddedDatabaseCreator( storeDir, true );
        setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        configure( this, storeDir );
        return this;
    }

    @Override
    protected DatabaseManagementService newEmbeddedDatabase( File storeDir, Config config, ExternalDependencies dependencies, boolean impermanent )
    {
        return new TestDatabaseManagementServiceFactory( impermanent ).newFacade( storeDir, augmentConfig( config ),
                GraphDatabaseDependencies.newDependencies( dependencies ) );
    }

    private static void setConfig( Map<Setting<?>,String> config, DatabaseManagementServiceBuilder builder )
    {
        for ( Map.Entry<Setting<?>,String> entry : config.entrySet() )
        {
            Setting<?> key = entry.getKey();
            String value = entry.getValue();
            builder.setConfig( key, value );
        }
    }
}
