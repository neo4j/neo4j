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
package org.neo4j.server;

import org.junit.After;
import org.junit.Test;

import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;

public class AcceptorConfigurationIT extends ExclusiveWebContainerTestBase
{

    private TestWebContainer testWebContainer;

    @After
    public void stopTheServer()
    {
        testWebContainer.shutdown();
    }

    @Test
    public void serverShouldNotHangWithThreadPoolSizeSmallerThanCpuCount() throws Exception
    {
        testWebContainer = serverOnRandomPorts().withMaxJettyThreads( 3 )
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();

        assertThat( GET( testWebContainer.getBaseUri().toString()).status(), is( 200 ) );
    }
}
