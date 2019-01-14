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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.query.QueryEngineProvider.noEngine;

/**
 * This is the main factory for creating database instances. It delegates creation to three different modules
 * ({@link PlatformModule}, {@link EditionModule}, and {@link DataSourceModule}),
 * which create all the specific services needed to run a graph database.
 * <p>
 * It is abstract in order for subclasses to specify their own {@link org.neo4j.kernel.impl.factory.EditionModule}
 * implementations. Subclasses also have to set the edition name in overridden version of
 * {@link #initFacade(File, Map, GraphDatabaseFacadeFactory.Dependencies, GraphDatabaseFacade)},
 * which is used for logging and similar.
 * <p>
 * To create test versions of databases, override an edition factory (e.g. {@link org.neo4j.kernel.impl.factory
 * .CommunityFacadeFactory}), and replace modules
 * with custom versions that instantiate alternative services.
 */
public class GraphDatabaseFacadeFactory
{
    public interface Dependencies
    {
        /**
         * Allowed to be null. Null means that no external {@link org.neo4j.kernel.monitoring.Monitors} was created,
         * let the
         * database create its own monitors instance.
         */
        Monitors monitors();

        LogProvider userLogProvider();

        Iterable<Class<?>> settingsClasses();

        Iterable<KernelExtensionFactory<?>> kernelExtensions();

        Map<String,URLAccessRule> urlAccessRules();

        Iterable<QueryEngineProvider> executionEngines();
    }

    public static class Configuration implements LoadableConfig
    {
        @Internal
        public static final Setting<Boolean> ephemeral =
                setting( "unsupported.dbms.ephemeral", BOOLEAN, Settings.FALSE );

        @Internal
        public static final Setting<String> lock_manager =
                setting( "unsupported.dbms.lock_manager", STRING, "" );

        @Internal
        public static final Setting<String> tracer =
                setting( "unsupported.dbms.tracer", STRING, Settings.NO_DEFAULT );

        @Internal
        public static final Setting<String> editionName =
                setting( "unsupported.dbms.edition", STRING, Edition.unknown.toString() );
    }

    protected final DatabaseInfo databaseInfo;
    private final Function<PlatformModule,EditionModule> editionFactory;

    public GraphDatabaseFacadeFactory( DatabaseInfo databaseInfo,
            Function<PlatformModule,EditionModule> editionFactory )
    {
        this.databaseInfo = databaseInfo;
        this.editionFactory = editionFactory;
    }

    /**
     * Instantiate a graph database given configuration and dependencies.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the newly constructed {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade newFacade( File storeDir, Config config, final Dependencies dependencies )
    {
        return initFacade( storeDir, config, dependencies, new GraphDatabaseFacade() );
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param params configuration parameters
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @param graphDatabaseFacade the already created facade which needs initialisation
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade initFacade( File storeDir, Map<String,String> params, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        return initFacade( storeDir, Config.defaults( params ), dependencies, graphDatabaseFacade );
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @param graphDatabaseFacade the already created facade which needs initialisation
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade initFacade( File storeDir, Config config, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        PlatformModule platform = createPlatform( storeDir, config, dependencies, graphDatabaseFacade );
        EditionModule edition = editionFactory.apply( platform );

        AtomicReference<QueryExecutionEngine> queryEngine = new AtomicReference<>( noEngine() );

        final DataSourceModule dataSource = createDataSource( platform, edition, queryEngine::get );
        Logger msgLog = platform.logging.getInternalLog( getClass() ).infoLogger();
        CoreAPIAvailabilityGuard coreAPIAvailabilityGuard = edition.coreAPIAvailabilityGuard;

        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, msgLog, coreAPIAvailabilityGuard );
        graphDatabaseFacade.init(
                edition,
                spi,
                dataSource.guard,
                dataSource.threadToTransactionBridge,
                platform.config,
                edition.relationshipTypeTokenHolder
        );

        // Start it
        platform.dataSourceManager.addListener( new DataSourceManager.Listener()
        {
            private QueryExecutionEngine engine;

            @Override
            public void registered( NeoStoreDataSource dataSource )
            {
                if ( engine == null )
                {
                    engine = QueryEngineProvider.initialize(
                            platform.dependencies, platform.graphDatabaseFacade, dependencies.executionEngines()
                    );
                }

                queryEngine.set( engine );
            }

            @Override
            public void unregistered( NeoStoreDataSource dataSource )
            {
                queryEngine.set( noEngine() );
            }
        } );

        RuntimeException error = null;
        try
        {
            // Done after create to avoid a redundant
            // "database is now unavailable"
            enableAvailabilityLogging( platform.availabilityGuard, msgLog );

            platform.life.start();
        }
        catch ( final Throwable throwable )
        {
            error = new RuntimeException( "Error starting " + getClass().getName() + ", " +
                    platform.storeDir, throwable );
        }
        finally
        {
            if ( error != null )
            {
                try
                {
                    graphDatabaseFacade.shutdown();
                }
                catch ( Throwable shutdownError )
                {
                    error.addSuppressed( shutdownError );
                }
            }
        }

        if ( error != null )
        {
            msgLog.log( "Failed to start database", error );
            throw error;
        }

        return graphDatabaseFacade;
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected PlatformModule createPlatform( File storeDir, Config config, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade );
    }

    /**
     * Create the datasource module. Override to replace with custom module.
     */
    protected DataSourceModule createDataSource(
            PlatformModule platformModule,
            EditionModule editionModule,
            Supplier<QueryExecutionEngine> queryEngine )
    {
        return new DataSourceModule( platformModule, editionModule, queryEngine );
    }

    private void enableAvailabilityLogging( AvailabilityGuard availabilityGuard, final Logger msgLog )
    {
        availabilityGuard.addListener( new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
                msgLog.log( "Database is now ready" );
            }

            @Override
            public void unavailable()
            {
                msgLog.log( "Database is now unavailable" );
            }
        } );
    }
}
