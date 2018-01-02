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

import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.database.Database;
import org.neo4j.server.web.WebServer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThirdPartyJAXRSModuleTest
{
    @Test
    public void shouldReportThirdPartyPackagesAtSpecifiedMount() throws Exception
    {
        // Given
        WebServer webServer = mock( WebServer.class );

        CommunityNeoServer neoServer = mock( CommunityNeoServer.class );
        when( neoServer.baseUri() ).thenReturn( new URI( "http://localhost:7575" ) );
        when( neoServer.getWebServer() ).thenReturn( webServer );
        when( neoServer.getDatabase() ).thenReturn( mock(Database.class));
        
        Config config = mock( Config.class );
        List<ThirdPartyJaxRsPackage> jaxRsPackages = new ArrayList<ThirdPartyJaxRsPackage>();
        String path = "/third/party/package";
        jaxRsPackages.add( new ThirdPartyJaxRsPackage( "org.example.neo4j", path ) );
        when( config.get( ServerSettings.third_party_packages ) ).thenReturn( jaxRsPackages );

        // When
        ThirdPartyJAXRSModule module = new ThirdPartyJAXRSModule(webServer, config, NullLogProvider.getInstance(), neoServer );
        module.start();

        // Then
        verify( webServer ).addJAXRSPackages( any( List.class ), anyString(), anyCollection() );
    }
}
