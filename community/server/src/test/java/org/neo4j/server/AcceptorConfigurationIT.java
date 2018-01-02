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
package org.neo4j.server;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.server;
import static org.neo4j.test.server.HTTP.GET;

import org.junit.After;
import org.junit.Test;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class AcceptorConfigurationIT extends ExclusiveServerTestBase
{

    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void serverShouldNotHangWithThreadPoolSizeSmallerThanCpuCount() throws Exception
    {
        server = server().withMaxJettyThreads( 3 )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        assertThat( GET(server.baseUri().toString()).status(), is( 200 ) );
    }
}
