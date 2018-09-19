/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise;

import org.eclipse.jetty.util.thread.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.logging.LogProvider;
import org.neo4j.metrics.source.server.ServerThreadView;
import org.neo4j.metrics.source.server.ServerThreadViewSetter;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.EnterpriseGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import org.neo4j.server.enterprise.modules.JMXManagementModule;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.DatabaseRoleInfoServerModule;
import org.neo4j.server.rest.EnterpriseDiscoverableURIs;
import org.neo4j.server.rest.MasterInfoService;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.configuration.ServerSettings.jmx_module_enabled;

public class OpenEnterpriseNeoServer extends CommunityNeoServer
{
    public OpenEnterpriseNeoServer( Config config, Dependencies dependencies )
    {
        super( config, new EnterpriseGraphFactory(), dependencies );
    }

    public OpenEnterpriseNeoServer( Config config, GraphFactory graphFactory, Dependencies dependencies )
    {
        super( config, graphFactory, dependencies );
    }

    @Override
    protected WebServer createWebServer()
    {
        Jetty9WebServer webServer = (Jetty9WebServer) super.createWebServer();
        webServer.setJettyCreatedCallback( jetty ->
        {
            ThreadPool threadPool = jetty.getThreadPool();
            assert threadPool != null;
            try
            {
                ServerThreadViewSetter setter =
                        database.getGraph().getDependencyResolver().resolveDependency( ServerThreadViewSetter.class );
                setter.set( new ServerThreadView()
                {
                    @Override
                    public int allThreads()
                    {
                        return threadPool.getThreads();
                    }

                    @Override
                    public int idleThreads()
                    {
                        return threadPool.getIdleThreads();
                    }
                } );
            }
            catch ( UnsatisfiedDependencyException ex )
            {
                // nevermind, metrics are likely not enabled
            }
        } );
        return webServer;
    }

    @Override
    protected AuthorizationModule createAuthorizationModule()
    {
        return new EnterpriseAuthorizationModule( webServer, authManagerSupplier, userLogProvider, getConfig(),
                getUriWhitelist() );
    }

    @Override
    protected DBMSModule createDBMSModule()
    {
        // ConnectorPortRegister isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs  = () -> EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs(
                        getConfig(), getDependencyResolver().resolveDependency( ConnectorPortRegister.class ) );
        return new DBMSModule( webServer, getConfig(), discoverableURIs );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        List<ServerModule> modules = new ArrayList<>();
        modules.add( new DatabaseRoleInfoServerModule( webServer, getConfig(), userLogProvider ) );
        if ( getConfig().get( jmx_module_enabled ) )
        {
            modules.add( new JMXManagementModule( this ) );
        }
        super.createServerModules().forEach( modules::add );
        return modules;
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

    @Override
    protected Pattern[] getUriWhitelist()
    {
        final List<Pattern> uriWhitelist = new ArrayList<>( Arrays.asList( super.getUriWhitelist() ) );

        if ( !getConfig().get( HaSettings.ha_status_auth_enabled ) )
        {
            uriWhitelist.add( Pattern.compile( "/db/manage/server/ha.*" ) );
        }

        if ( !getConfig().get( CausalClusteringSettings.status_auth_enabled ) )
        {
            uriWhitelist.add( Pattern.compile( "/db/manage/server/core.*" ) );
            uriWhitelist.add( Pattern.compile( "/db/manage/server/read-replica.*" ) );
        }

        return uriWhitelist.toArray( new Pattern[uriWhitelist.size()] );
    }
}
