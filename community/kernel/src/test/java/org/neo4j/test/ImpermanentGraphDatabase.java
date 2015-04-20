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
package org.neo4j.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.kernel.InternalAbstractGraphDatabase.Configuration.ephemeral;
import static org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent;

/**
 * A database meant to be used in unit tests. It will always be empty on start.
 */
public class ImpermanentGraphDatabase extends EmbeddedGraphDatabase
{
    /**
     * If enabled will track unclosed database instances in tests. The place of instantiation
     * will get printed in an exception with the message "Unclosed database instance".
     */
    private static boolean TRACK_UNCLOSED_DATABASE_INSTANCES = false;
    private static final Map<File, Exception> startedButNotYetClosed = new ConcurrentHashMap<>();

    protected static final String PATH = "target/test-data/impermanent-db";

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase()
    {
        this( new HashMap<String, String>() );
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
    public ImpermanentGraphDatabase( String storeDir )
    {
        this( storeDir, withForcedInMemoryConfiguration( new HashMap<String, String>() ) );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( Map<String, String> params )
    {
        this( PATH, withForcedInMemoryConfiguration( params ) );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( String storeDir, Map<String, String> params )
    {
        this( storeDir, withForcedInMemoryConfiguration( params ),
                Iterables.<KernelExtensionFactory<?>, KernelExtensionFactory>cast( Service.load(
                        KernelExtensionFactory.class ) ),
                Service.load( CacheProvider.class )
        );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( Map<String, String> params,
                                     Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                     Iterable<CacheProvider> cacheProviders )
    {
        this( PATH, params, kernelExtensions, cacheProviders );
    }

    /**
     * This is deprecated. Use {@link TestGraphDatabaseFactory} instead
     */
    @Deprecated
    public ImpermanentGraphDatabase( String storeDir, Map<String, String> params,
                                     Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                     Iterable<CacheProvider> cacheProviders )
    {
        this( storeDir, withForcedInMemoryConfiguration( params ),
                getDependencies( kernelExtensions, cacheProviders ) );
    }

    private static Dependencies getDependencies(
            Iterable<KernelExtensionFactory<?>> kernelExtensions,
            Iterable<CacheProvider> cacheProviders )
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        state.setKernelExtensions( kernelExtensions );
        state.setCacheProviders( cacheProviders );
        return state.databaseDependencies();
    }

    public ImpermanentGraphDatabase( String storeDir, Map<String, String> params, Dependencies dependencies )
    {
        super( storeDir, withForcedInMemoryConfiguration( params ), dependencies );
        trackUnclosedUse( storeDir );
    }

    private void trackUnclosedUse( String path )
    {
        if ( TRACK_UNCLOSED_DATABASE_INSTANCES )
        {
            Exception testThatDidNotCloseDb = startedButNotYetClosed.put( new File( path ),
                    new Exception( "Unclosed database instance" ) );
            if ( testThatDidNotCloseDb != null )
            {
                testThatDidNotCloseDb.printStackTrace();
            }
        }
    }

    @Override
    protected DiagnosticsManager createDiagnosticsManager()
    {
        return new DiagnosticsManager( StringLogger.DEV_NULL )
        {
            @Override
            public void init() throws Throwable
            {
                // Do nothing
            }

            @Override
            public void start()
            {
                // Do nothing
            }

            @Override
            public void stop() throws Throwable
            {
                // Do nothing
            }

            @Override
            public void shutdown()
            {
                // Do nothing
            }
        };
    }

    @Override
    public void shutdown()
    {
        if ( TRACK_UNCLOSED_DATABASE_INSTANCES )
        {
            startedButNotYetClosed.remove( new File( getStoreDir() ) );
        }

        super.shutdown();
    }

    @Override
    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return life.add( new EphemeralFileSystemAbstraction() );
    }

    private static Map<String, String> withForcedInMemoryConfiguration( Map<String, String> params )
    {
        Map<String, String> result = new HashMap<>( params );
        // To signal to index provides that we should be in-memory
        result.put( ephemeral.name(), TRUE );
        if ( !result.containsKey( pagecache_memory.name() ) )
        {
            result.put( pagecache_memory.name(), "8M" );
        }
        return result;
    }

    @Override
    protected Logging createLogging()
    {
        return life.add( new SingleLoggingService( StringLogger.loggerDirectory( fileSystem, storeDir ) ) );
    }

    public void cleanContent()
    {
        cleanDatabaseContent( this );
    }
}
