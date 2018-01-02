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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class NeoServerShutdownLoggingDocIT extends ExclusiveServerTestBase
{
    private AssertableLogProvider logProvider;
    private NeoServer server;

    @Before
    public void setupServer() throws IOException
    {
        logProvider = new AssertableLogProvider();
        server = ServerHelper.createNonPersistentServer( logProvider );
        ServerHelper.cleanTheDatabase( server );
    }

    @After
    public void shutdownTheServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void shouldLogShutdown() throws Exception
    {
        server.stop();
        logProvider.assertContainsMessageContaining( "Successfully shutdown database" );
    }
}
