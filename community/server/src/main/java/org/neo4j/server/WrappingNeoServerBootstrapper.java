/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.EmbeddedServerConfigurator;
import org.neo4j.server.database.GraphDatabaseFactory;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.modules.DiscoveryModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckRule;

/**
 * A bootstrapper for the Neo4j Server that takes an already instantiated
 * {@link AbstractGraphDatabase}, and optional configuration, and launches a
 * server using that database.
 * <p>
 * Use this to start up a full Neo4j server from within an application that
 * already uses the {@link EmbeddedGraphDatabase} or the
 * {@link HighlyAvailableGraphDatabase}. This gives your application the full
 * benefits of the server's REST API, the Web administration interface and
 * statistics tracking.
 * <p>
 * Example:
 * 
 * <pre>
 * {
 *     &#064;code WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper( myDatabase );
 *     srv.start(); // Launches the server at default URL, http://localhost:7474
 * 
 *     // Run your application as long as you please
 * 
 *     srv.stop();
 * }
 * </pre>
 * 
 * If you want to change configuration, pass in the optional Configurator arg to
 * the constructor. You can write your own implementation or use
 * {@link EmbeddedServerConfigurator}.
 */
public class WrappingNeoServerBootstrapper extends Bootstrapper
{

    private final AbstractGraphDatabase db;
    private final Configurator configurator;
    private static Logger log = Logger.getLogger( WrappingNeoServerBootstrapper.class );

    /**
     * Create an instance with default settings.
     * 
     * @param db
     */
    public WrappingNeoServerBootstrapper( AbstractGraphDatabase db )
    {
        this( db, new EmbeddedServerConfigurator( db ) );
    }

    /**
     * Create an instance with custom documentation.
     * {@link EmbeddedServerConfigurator} is written to fit well here, see its'
     * documentation.
     * 
     * @param db
     * @param configurator
     */
    public WrappingNeoServerBootstrapper( AbstractGraphDatabase db, Configurator configurator )
    {
        this.db = db;
        this.configurator = configurator;
    }

    @Override
    public Iterable<StartupHealthCheckRule> getHealthCheckRules()
    {
        return Arrays.asList();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public Iterable<Class<? extends ServerModule>> getServerModules()
    {
        return Arrays.asList( DiscoveryModule.class, RESTApiModule.class, ManagementApiModule.class,
                ThirdPartyJAXRSModule.class, WebAdminModule.class );
    }

    @Override
    public int stop( int stopArg )
    {
        try
        {
            if ( server != null )
            {
                server.stopServer();
                server.getDatabase()
                        .rrdDb()
                        .close();
            }
            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%d]. Reason [%s] ", server.getWebServerPort(),
                    e.getMessage() );
            return 1;
        }
    }

    @Override
    protected void addShutdownHook()
    {
        // No-op
    }

    @Override
    protected GraphDatabaseFactory getGraphDatabaseFactory( Configuration configuration )
    {
        return new GraphDatabaseFactory()
        {
            @Override
            public AbstractGraphDatabase createDatabase( String databaseStoreDirectory,
                    Map<String, String> databaseProperties )
            {
                return db;
            }
        };

    }

    @Override
    protected Configurator getConfigurator()
    {
        return configurator;
    }
}
