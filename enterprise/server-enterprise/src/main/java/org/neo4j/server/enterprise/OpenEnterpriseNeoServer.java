/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.eclipse.jetty.util.thread.ThreadPool;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.EnterpriseGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.logging.LogProvider;
import org.neo4j.metrics.source.server.ServerThreadView;
import org.neo4j.metrics.source.server.ServerThreadViewSetter;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import org.neo4j.server.enterprise.modules.JMXManagementModule;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.DatabaseRoleInfoServerModule;
import org.neo4j.server.rest.MasterInfoService;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.configuration.ServerSettings.jmx_module_enabled;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class OpenEnterpriseNeoServer extends CommunityNeoServer
{

    private static final GraphFactory HA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new HighlyAvailableGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory ENTERPRISE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new EnterpriseGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory CORE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new CoreGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory READ_REPLICA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new ReadReplicaGraphDatabase( storeDir, config, dependencies );
    };

    public OpenEnterpriseNeoServer( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        super( config, createDbFactory( config ), dependencies, logProvider );
    }

    public OpenEnterpriseNeoServer( Config config, Database.Factory dbFactory, GraphDatabaseFacadeFactory.Dependencies
            dependencies, LogProvider logProvider )
    {
        super( config, dbFactory, dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final Mode mode = config.get( EnterpriseEditionSettings.mode );

        switch ( mode )
        {
        case HA:
            return lifecycleManagingDatabase( HA_FACTORY );
        case ARBITER:
            // Should never reach here because this mode is handled separately by the scripts.
            throw new IllegalArgumentException( "The server cannot be started in ARBITER mode." );
        case CORE:
            return lifecycleManagingDatabase( CORE_FACTORY );
        case READ_REPLICA:
            return lifecycleManagingDatabase( READ_REPLICA_FACTORY );
        default:
            return lifecycleManagingDatabase( ENTERPRISE_FACTORY );
        }
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
        return new EnterpriseAuthorizationModule( webServer, authManagerSupplier, logProvider, getConfig(),
                getUriWhitelist() );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        List<ServerModule> modules = new ArrayList<>();
        modules.add( new DatabaseRoleInfoServerModule( webServer, getConfig(), logProvider ) );
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
