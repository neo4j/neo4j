/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.SuppressOutput;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class DBMSModuleTest
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.err,
            SuppressOutput.System.out );
    
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldRegisterAtRootByDefault() throws Exception
    {
        WebServer webServer = mock( WebServer.class );

        CommunityNeoServer neoServer = mock( CommunityNeoServer.class );
        when( neoServer.baseUri() ).thenReturn( new URI( "http://localhost:7575" ) );
        when( neoServer.getWebServer() ).thenReturn( webServer );

        DBMSModule module = new DBMSModule( webServer );

        module.start();

        verify( webServer ).addJAXRSClasses( any( List.class ), anyString(), anyCollection() );
    }
}
