/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.graphdb.EnterpriseGraphDatabase;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.webadmin.rest.DatabaseRoleInfoServerModule;
import org.neo4j.server.webadmin.rest.MasterInfoService;

import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.Iterables.mix;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class EnterpriseNeoServer extends AdvancedNeoServer
{
    public enum Mode
    {
        SINGLE,
        HA,
        CORE,
        EDGE;

        public static Mode fromString( String value )
        {
            try
            {
                return Mode.valueOf( value );
            }
            catch ( IllegalArgumentException ex )
            {
                return SINGLE;
            }
        }

    }

    private static final GraphFactory HA_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {

            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new HighlyAvailableGraphDatabase( storeDir, config.getParams(), dependencies );
        }
    };
    private static final GraphFactory ENTERPRISE_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {
            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new EnterpriseGraphDatabase( storeDir, config.getParams(), dependencies );
        }
    };

    private static final GraphFactory CORE_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {
            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new CoreGraphDatabase( storeDir, config.getParams(), dependencies );
        }
    };

    private static final GraphFactory EDGE_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {
            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new EdgeGraphDatabase( storeDir, config.getParams(), dependencies );
        }
    };

    public EnterpriseNeoServer( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        super( config, createDbFactory( config ), dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final Mode mode = Mode.fromString( config.get( EnterpriseServerSettings.mode ).toUpperCase() );

        switch ( mode )
        {
        case HA:
            return lifecycleManagingDatabase( HA_FACTORY );
        case CORE:
            return lifecycleManagingDatabase( CORE_FACTORY );
        case EDGE:
            return lifecycleManagingDatabase( EDGE_FACTORY );
        default: // Anything else gives community, including Mode.SINGLE
            return lifecycleManagingDatabase( ENTERPRISE_FACTORY );
        }
    }


    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return mix(
                asList( (ServerModule) new DatabaseRoleInfoServerModule( webServer, getConfig(),
                        logProvider ) ), super.createServerModules() );
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
        return uriWhitelist.toArray( new Pattern[uriWhitelist.size()] );
    }
}
