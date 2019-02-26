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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.common.Service;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.facade.embedded.EmbeddedGraphDatabase;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;

import static org.neo4j.configuration.GraphDatabaseSettings.ephemeral;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

/**
 * A database meant to be used in unit tests. It will always be empty on start.
 */
public class ImpermanentGraphDatabase extends EmbeddedGraphDatabase
{
    /**
     * If enabled will track unclosed database instances in tests. The place of instantiation
     * will get printed in an exception with the message "Unclosed database instance".
     */
    private static final boolean TRACK_UNCLOSED_DATABASE_INSTANCES = false;
    private static final Map<File, Exception> startedButNotYetClosed = new ConcurrentHashMap<>();

    protected static final File PATH = new File( "target/test-data/impermanent-db" );

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase()
    {
        this( new HashMap<>() );
    }

    /*
     * TODO this shouldn't be here. It so happens however that some tests may use the database
     * directory as the path to store stuff and in this case we need to define the path explicitly,
     * otherwise we end up writing outside the workspace and hence leave stuff behind.
     * The other option is to explicitly remove all files present on startup. Either way,
     * the fact that this discussion takes place is indication that things are inconsistent,
     * since an ImpermanentGraphDatabase should not have any mention of a store directory in
     * any case.
     */
    public ImpermanentGraphDatabase( File storeDir )
    {
        this( storeDir, new HashMap<>() );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( Map<String, String> params )
    {
        this( PATH, params );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( File storeDir, Map<String, String> params )
    {
        this( storeDir, params,
                Iterables.cast( Service.loadAll( ExtensionFactory.class ) ) );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( Map<String, String> params,
                                     Iterable<ExtensionFactory<?>> extensions )
    {
        this( PATH, params, extensions );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( File storeDir, Map<String, String> params,
                                     Iterable<ExtensionFactory<?>> extensions )
    {
        this( storeDir, params, getDependencies( extensions ) );
    }

    private static ExternalDependencies getDependencies( Iterable<ExtensionFactory<?>> extensions )
    {
        return newDependencies().extensions( extensions );
    }

    public ImpermanentGraphDatabase( File storeDir, Map<String, String> params, ExternalDependencies dependencies )
    {
        super( storeDir, params, dependencies );
        trackUnclosedUse( storeDir );
    }

    public ImpermanentGraphDatabase( File storeDir, Config config,
            ExternalDependencies dependencies )
    {
        super( storeDir, config, dependencies );
        trackUnclosedUse( storeDir );
    }

    @Override
    protected void create( File storeDir, Map<String, String> params, ExternalDependencies dependencies )
    {
        new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
        {
            @Override
            protected GlobalModule createGlobalPlatform( File storeDir, Config config, ExternalDependencies dependencies )
            {
                return new ImpermanentGlobalModule( storeDir, config, databaseInfo, dependencies );
            }
        }.initFacade( storeDir, params, dependencies, this );
    }

    private static void trackUnclosedUse( File storeDir )
    {
        if ( TRACK_UNCLOSED_DATABASE_INSTANCES )
        {
            Exception testThatDidNotCloseDb = startedButNotYetClosed.put( storeDir,
                    new Exception( "Unclosed database instance" ) );
            if ( testThatDidNotCloseDb != null )
            {
                testThatDidNotCloseDb.printStackTrace();
            }
        }
    }

    @Override
    public void shutdown()
    {
        if ( TRACK_UNCLOSED_DATABASE_INSTANCES )
        {
            startedButNotYetClosed.remove( databaseLayout().databaseDirectory() );
        }

        super.shutdown();
    }

    private static Config withForcedInMemoryConfiguration( Config config )
    {
        config.augment( ephemeral, TRUE );
        config.augmentDefaults( pagecache_memory, "8M" );
        return config;
    }

    protected static class ImpermanentGlobalModule extends GlobalModule
    {
        public ImpermanentGlobalModule( File storeDir, Config config, DatabaseInfo databaseInfo, ExternalDependencies dependencies )
        {
            super( storeDir, withForcedInMemoryConfiguration(config), databaseInfo, dependencies );
        }

        @Override
        protected StoreLocker createStoreLocker()
        {
            return new StoreLocker( getFileSystem(), getStoreLayout() );
        }

        @Override
        protected FileSystemAbstraction createFileSystemAbstraction()
        {
            return new EphemeralFileSystemAbstraction();
        }

        @Override
        protected LogService createLogService( LogProvider userLogProvider )
        {
            return new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );
        }
    }
}
