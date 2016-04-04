/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.kernel.configuration.Settings.ANY;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.illegalValueMessage;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * This is the main factory for creating database instances. It delegates creation to three different modules
 * ({@link PlatformModule}, {@link EditionModule}, and {@link DataSourceModule}),
 * which create all the specific services needed to run a graph database.
 * <p/>
 * It is abstract in order for subclasses to specify their own {@link org.neo4j.kernel.impl.factory.EditionModule}
 * implementations. Subclasses also have to set the edition name
 * in overridden version of {@link #newFacade(File, Map, GraphDatabaseFacadeFactory.Dependencies, GraphDatabaseFacade)},
 * which is used for logging and similar.
 * <p/>
 * To create test versions of databases, override an edition factory (e.g. {@link org.neo4j.kernel.impl.factory
 * .CommunityFacadeFactory}), and replace modules
 * with custom versions that instantiate alternative services.
 */
public abstract class GraphDatabaseFacadeFactory
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

    public static class Configuration
    {
        public static final Setting<Boolean> ephemeral = setting( "unsupported.dbms.ephemeral", Settings.BOOLEAN, Settings.FALSE );
        public static final Setting<String> ephemeral_keep_logical_logs = setting( "dbms.tx_log.rotation.retention_policy", STRING, "1 " +
                "files", illegalValueMessage( "must be `true`/`false` or of format '<number><optional unit> <type>' " +
                "for example `100M size` for " +
                "limiting logical log space on disk to 100Mb," +
                " or `200k txs` for limiting the number of transactions to keep to 200 000", matches( ANY ) ) );

        // Kept here to have it not be publicly documented.
        public static final Setting<String> lock_manager = setting( "unsupported.dbms.lock_manager", Settings.STRING, "" );
        public static final Setting<String> record_format = setting( "unsupported.dbms.record_format", Settings.STRING, "" );
        public static final Setting<String> tracer =
                setting( "unsupported.dbms.tracer", Settings.STRING, (String) null ); // 'null' default.

        public static final Setting<String> editionName = setting( "unsupported.dbms.edition", Settings.STRING,
                Edition.unknown.toString() );
    }

    /**
     * Instantiate a graph database given configuration and dependencies.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param params configuration parameters
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the newly constructed {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade newFacade( File storeDir, Map<String, String> params, final Dependencies dependencies )
    {
        return newFacade( storeDir, params, dependencies, new GraphDatabaseFacade() );
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
    public GraphDatabaseFacade newFacade( File storeDir, Map<String, String> params, final Dependencies dependencies,
                                          final GraphDatabaseFacade graphDatabaseFacade )
    {
        PlatformModule platform = createPlatform( storeDir, params, dependencies, graphDatabaseFacade );
        EditionModule edition = createEdition( platform );
        final DataSourceModule dataSource = createDataSource( dependencies, platform, edition );
        Logger msgLog = platform.logging.getInternalLog( getClass() ).infoLogger();
        CoreAPIAvailabilityGuard coreAPIAvailabilityGuard = edition.coreAPIAvailabilityGuard;

        // Start it
        graphDatabaseFacade.init( new ClassicCoreSPI( platform, dataSource, msgLog, coreAPIAvailabilityGuard ) );

        Throwable error = null;
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
                    error = Exceptions.withSuppressed( shutdownError, error );
                }
            }
        }

        if ( error != null )
        {
            msgLog.log( "Failed to start database", error );
            throw Exceptions.launderedException( error );
        }

        return graphDatabaseFacade;
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected PlatformModule createPlatform( File storeDir, Map<String, String> params, final Dependencies dependencies,
                                             final GraphDatabaseFacade graphDatabaseFacade )
    {
        return new PlatformModule( storeDir, params, databaseInfo(), dependencies, graphDatabaseFacade );
    }

    /**
     * Create the edition module. Implement to provide the edition services specified by the public fields in {@link
     * org.neo4j.kernel.impl.factory.EditionModule}.
     */
    protected abstract EditionModule createEdition( PlatformModule platformModule );

    /**
     * Create the datasource module. Override to replace with custom module.
     */
    protected DataSourceModule createDataSource( final Dependencies dependencies,
                                                 final PlatformModule platformModule, EditionModule editionModule )
    {
        return new DataSourceModule( dependencies, platformModule, editionModule );
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

    public int selectionPriority()
    {
        return Integer.MIN_VALUE;
    }

    public abstract DatabaseInfo databaseInfo();
}
