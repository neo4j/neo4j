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
package org.neo4j.server.modules;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.web.WebServer;
import org.neo4j.udc.UsageData;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RESTApiModuleTest
{
    @Test
    public void shouldRegisterASingleUri() throws Exception
    {
        // Given
        WebServer webServer = mock( WebServer.class );

        Map<String, String> params = new HashMap();
        String path = "/db/data";
        params.put( ServerSettings.rest_api_path.name(), path );
        Config config = new Config( params );

        Dependencies deps = new Dependencies();
        deps.satisfyDependency( new UsageData( mock( JobScheduler.class ) ) );

        Database db = mock(Database.class);

        // When
        RESTApiModule module = new RESTApiModule( webServer, config, deps, NullLogProvider.getInstance() );
        module.start();

        // Then
        verify( webServer ).addJAXRSClasses( any( List.class ), anyString(), anyCollection() );
    }
}
