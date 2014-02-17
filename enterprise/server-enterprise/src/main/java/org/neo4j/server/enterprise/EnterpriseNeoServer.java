/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise;

import org.apache.commons.configuration.Configuration;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.InterruptThreadTimer;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.PerformRecoveryIfNecessary;
import org.neo4j.server.preflight.PerformUpgradeIfNecessary;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.webadmin.rest.AdvertisableService;
import org.neo4j.server.webadmin.rest.MasterInfoServerModule;
import org.neo4j.server.webadmin.rest.MasterInfoService;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.mix;
import static org.neo4j.server.configuration.Configurator.DB_MODE_KEY;
import static org.neo4j.server.database.LifecycleManagingDatabase.EMBEDDED;
import static org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class EnterpriseNeoServer extends AdvancedNeoServer
{
    public static final String SINGLE = "SINGLE";
    public static final String HA = "HA";

    private static final GraphFactory HA_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Function<Config, Logging> loggingProvider,
                                                  Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                                  Iterable<CacheProvider> cacheProviders,
                                                  Iterable<TransactionInterceptorProvider> txInterceptorProviders )
        {
            return new HighlyAvailableGraphDatabase( config, loggingProvider,
                kernelExtensions, cacheProviders, txInterceptorProviders );
        }
    };

    public EnterpriseNeoServer( Configurator configurator )
    {
        super( configurator, createDbFactory( configurator.configuration() ) );
    }

    public EnterpriseNeoServer( Configurator configurator, Database.Factory dbFactory )
    {
        super( configurator, dbFactory );
    }

    protected static Database.Factory createDbFactory( Configuration config )
    {
        String mode = config.getString( DB_MODE_KEY, SINGLE ).toUpperCase();
        return mode.equals(HA) ?
            lifecycleManagingDatabase( HA_FACTORY ) :
            lifecycleManagingDatabase( EMBEDDED );
    }

    @Override
    protected PreFlightTasks createPreflightTasks()
    {
        return new PreFlightTasks(
                // TODO: This check should be done in the bootrapper,
                // and verification of config should be done by the new
                // config system.
                //new EnsureEnterpriseNeo4jPropertiesExist(configurator.configuration()),
                new EnsurePreparedForHttpLogging(configurator.configuration()),
                new PerformUpgradeIfNecessary(getConfiguration(),
                        configurator.getDatabaseTuningProperties(), System.out),
                new PerformRecoveryIfNecessary(getConfiguration(),
                        configurator.getDatabaseTuningProperties(), System.out));
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return mix( asList(
                (ServerModule) new MasterInfoServerModule( webServer, getConfiguration() ) ),
                super.createServerModules() );
    }

    @Override
    protected InterruptThreadTimer createInterruptStartupTimer()
    {
        // If we are in HA mode, database startup can take a very long time, so
        // we default to disabling the startup timeout here, unless explicitly overridden
        // by configuration.
        if(getConfiguration().getString( DB_MODE_KEY, "single" ).equalsIgnoreCase("ha"))
        {
            long startupTimeout = getConfiguration().getInt(Configurator.STARTUP_TIMEOUT, 0) * 1000;
            InterruptThreadTimer stopStartupTimer;
            if(startupTimeout > 0)
            {
                stopStartupTimer = InterruptThreadTimer.createTimer(
                        startupTimeout,
                        Thread.currentThread());
            } else
            {
                stopStartupTimer = InterruptThreadTimer.createNoOpTimer();
            }
            return stopStartupTimer;
        } else
        {
            return super.createInterruptStartupTimer();
        }
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        if ( getDatabase().getGraph() instanceof HighlyAvailableGraphDatabase )
        {
            return Iterables.append( new MasterInfoService( null, null ), super.getServices() );
        }
        else
        {
            return super.getServices();
        }
    }
}
