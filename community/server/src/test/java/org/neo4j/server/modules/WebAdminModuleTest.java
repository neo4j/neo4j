/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.modules;

import java.net.URI;
import java.util.HashMap;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.RrdDbWrapper;
import org.neo4j.server.web.WebServer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebAdminModuleTest
{
    @Test
    public void shouldRegisterRrdDb() throws Exception
    {
        WebServer webServer = mock( WebServer.class );

        CommunityNeoServer neoServer = mock( CommunityNeoServer.class );
        when( neoServer.baseUri() ).thenReturn( new URI( "http://localhost:7575" ) );
        when( neoServer.getWebServer() ).thenReturn( webServer );
        when( neoServer.getConfiguration() ).thenReturn( new MapConfiguration( new HashMap<Object, Object>() ) );

        Database db = mock( Database.class );
        when( neoServer.getDatabase() ).thenReturn( db );

        AbstractGraphDatabase graph = mock( AbstractGraphDatabase.class );
        when( db.getGraph() ).thenReturn( graph );

        DependencyResolver resolver = mock( DependencyResolver.class );
        when( graph.getDependencyResolver() ).thenReturn( resolver );

        NodeManager nodeManagerMock = mock( NodeManager.class );
        when( resolver.resolveDependency( NodeManager.class ) ).thenReturn( nodeManagerMock );

        WebAdminModule module = new WebAdminModule( webServer, neoServer.getConfiguration(), db );
        module.start( StringLogger.DEV_NULL );

        verify( db ).setRrdDb( any( RrdDbWrapper.class ) );
    }
}
