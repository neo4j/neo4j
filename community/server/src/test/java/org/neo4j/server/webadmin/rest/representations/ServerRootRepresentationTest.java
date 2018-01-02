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
package org.neo4j.server.webadmin.rest.representations;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.rest.management.repr.ServerRootRepresentation;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ServerRootRepresentationTest
{
    @Test
    public void shouldProvideAListOfServiceUris() throws Exception
    {
        ConsoleService consoleService = new ConsoleService( null, mock( Database.class ), NullLogProvider.getInstance(), null );
        ServerRootRepresentation srr = new ServerRootRepresentation( new URI( "http://example.org:9999" ),
                Collections.<AdvertisableService>singletonList( consoleService ) );
        Map<String, Map<String, String>> map = srr.serialize();

        assertNotNull( map.get( "services" ) );

        assertThat( map.get( "services" )
                .get( consoleService.getName() ), containsString( consoleService.getServerPath() ) );
    }
}
