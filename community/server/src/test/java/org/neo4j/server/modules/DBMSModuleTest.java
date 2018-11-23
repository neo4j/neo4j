/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.modules;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.rest.dbms.UserService;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DBMSModuleTest
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.err,
            SuppressOutput.System.out );

    @Test
    public void shouldRegisterAtRootByDefault() throws Exception
    {
        WebServer webServer = mock( WebServer.class );
        Config config = mock( Config.class );

        CommunityNeoServer neoServer = mock( CommunityNeoServer.class );
        when( neoServer.baseUri() ).thenReturn( new URI( "http://localhost:7575" ) );
        when( neoServer.getWebServer() ).thenReturn( webServer );
        when( config.get( GraphDatabaseSettings.auth_enabled ) ).thenReturn( true );

        DBMSModule module = new DBMSModule( webServer, config, () -> new DiscoverableURIs.Builder().build() );

        module.start();

        verify( webServer ).addJAXRSClasses( anyList(), anyString(), isNull() );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotRegisterUserServiceWhenAuthDisabled() throws Exception
    {
        WebServer webServer = mock( WebServer.class );
        Config config = mock( Config.class );

        CommunityNeoServer neoServer = mock( CommunityNeoServer.class );
        when( neoServer.baseUri() ).thenReturn( new URI( "http://localhost:7575" ) );
        when( neoServer.getWebServer() ).thenReturn( webServer );
        when( config.get( GraphDatabaseSettings.auth_enabled ) ).thenReturn( false );

        DBMSModule module = new DBMSModule( webServer, config, () -> new DiscoverableURIs.Builder().build() );

        module.start();

        verify( webServer ).addJAXRSClasses( anyList(), anyString(), isNull() );
        ArgumentCaptor<List<Class<?>>> captor = ArgumentCaptor.forClass( List.class );
        verify( webServer ).addJAXRSClasses( captor.capture(), anyString(), anyCollection() );

        List<Class<?>> registeredClasses = captor.getAllValues()
                .stream()
                .flatMap( List::stream )
                .collect( toList() );

        assertThat( registeredClasses, not( contains( UserService.class ) ) );
    }
}
