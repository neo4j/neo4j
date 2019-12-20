/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.web;

import org.junit.After;
import org.junit.Test;

import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

public class NeoWebServerDirectoryListingTestIT extends ExclusiveWebContainerTestBase
{
    private TestWebContainer testWebContainer;

    /*
     * The default jetty behaviour serves an index page for static resources. The 'directories' exposed through this
     * behaviour are not file system directories, but only a list of resources present on the classpath, so there is no
     * security vulnerability. However, it might seem like a vulnerability to somebody without the context of how the
     * whole stack works, so to avoid confusion we disable the jetty behaviour.
     */
    @Test
    public void shouldDisallowDirectoryListings() throws Exception
    {
        // Given
        testWebContainer = serverOnRandomPorts().build();

        // When
        Response okResource = HTTP.GET( testWebContainer.getBaseUri().resolve( "/browser/index.html" ).toString() );
        Response illegalResource = HTTP.GET( testWebContainer.getBaseUri().resolve( "/browser/files/" ).toString() );

        // Then
        // Depends on specific resources exposed by the browser module; if this test starts to fail,
        // check whether the structure of the browser module has changed and adjust accordingly.
        assertEquals( 200, okResource.status() );
        assertEquals( 404, illegalResource.status() );
    }

    @After
    public void cleanup()
    {
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
        }
    }

}
