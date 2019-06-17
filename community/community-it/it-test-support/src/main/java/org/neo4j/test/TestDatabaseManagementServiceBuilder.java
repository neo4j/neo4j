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
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.Preconditions;

/**
 * Test factory for graph databases.
 * Please be aware that since it's a database it will close filesystem as part of its lifecycle.
 * If you expect your file system to be open after database is closed, use {@link UncloseableDelegatingFileSystemAbstraction}
 */
public class TestDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
{
    private static final File EPHEMERAL_PATH = new File( "/target/test data/" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    public static final Predicate<ExtensionFactory<?>> INDEX_PROVIDERS_FILTER = extension -> extension instanceof AbstractIndexProviderFactory;

    protected FileSystemAbstraction fileSystem;
    protected LogProvider internalLogProvider;
    protected SystemNanoClock clock;
    protected boolean impermanent;
    private Config fromConfig;
    private boolean noOpSystemGraphInitializer;

    public TestDatabaseManagementServiceBuilder()
    {
        this( null );
        setUserLogProvider( NullLogProvider.getInstance() );
    }

    public TestDatabaseManagementServiceBuilder( File homeDirectory )
    {
        super( homeDirectory );
        setUserLogProvider( NullLogProvider.getInstance() );
    }

    @Override
    public DatabaseManagementService build()
    {
        Config cfg = config.set( GraphDatabaseSettings.neo4j_home, homeDirectory.toPath().toAbsolutePath() )
                .fromConfig( fromConfig )
                .build();

        if ( noOpSystemGraphInitializer )
        {
            dependencies = TestDatabaseIdRepository.noOpSystemGraphInitializer( dependencies, cfg );
        }

        return newDatabaseManagementService( homeDirectory, cfg, databaseDependencies() );
    }

    @Override
    protected DatabaseManagementService newDatabaseManagementService( File storeDir, Config config, ExternalDependencies dependencies )
    {
        Preconditions.checkArgument( storeDir != null || impermanent, "Database must have a root path or be impermanent." );
        return new TestDatabaseManagementServiceFactory( getDatabaseInfo(), getEditionFactory(), impermanent, fileSystem, clock, internalLogProvider )
                .build( storeDir, augmentConfig( config ), GraphDatabaseDependencies.newDependencies( dependencies ) );
    }

    @Override
    protected Config augmentConfig( Config config )
    {
        return Config.newBuilder()
                .fromConfig( config )
                .setDefault( GraphDatabaseSettings.pagecache_memory, "8m" )
                .setDefault( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes( 128 ) )
                .setDefault( BoltConnector.enabled, false )
                .build();
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

    public TestDatabaseManagementServiceBuilder setDatabaseRootDirectory( File storeDir )
    {
        this.homeDirectory = storeDir;
        return this;
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

    public TestDatabaseManagementServiceBuilder impermanent()
    {
        impermanent = true;
        if ( homeDirectory == null )
        {
            homeDirectory = EPHEMERAL_PATH;
        }
        return this;
    }

    public TestDatabaseManagementServiceBuilder setConfig( Config fromConfig )
    {
        if ( this.fromConfig != null )
        {
            throw new IllegalStateException( "You can only set config once." );
        }
        this.fromConfig = fromConfig;
        return this;
    }

    public DatabaseManagementServiceBuilder setConfigRaw( Map<String, String> raw )
    {
        config.setRaw( raw );
        return this;
    }

    public TestDatabaseManagementServiceBuilder noOpSystemGraphInitializer()
    {
        this.noOpSystemGraphInitializer = true;
        return this;
    }

    // Override to allow chaining

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

    @Override
    public TestDatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        return (TestDatabaseManagementServiceBuilder) super.addURLAccessRule( protocol, rule );
    }

    @Override
    public <T> TestDatabaseManagementServiceBuilder setConfig( Setting<T> setting, T value )
    {
        return (TestDatabaseManagementServiceBuilder) super.setConfig( setting, value );
    }

    @Override
    public TestDatabaseManagementServiceBuilder setConfig( Map<Setting<?>,Object> config )
    {
        return (TestDatabaseManagementServiceBuilder) super.setConfig( config );
    }

}
