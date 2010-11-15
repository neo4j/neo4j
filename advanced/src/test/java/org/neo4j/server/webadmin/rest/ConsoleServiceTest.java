/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.webadmin.rest;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.ConsoleSession;

import javax.ws.rs.core.Response;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class ConsoleServiceTest
{
    public ConsoleService consoleService;

    @Test
    public void retrievesTheReferenceNode()
    {
        Response evaluatedGremlinResponse = consoleService.exec( "{ \"command\" : \"$_\" }" );

        assertEquals(200, evaluatedGremlinResponse.getStatus());
        assertThat((String)evaluatedGremlinResponse.getEntity(), containsString("v[0]"));
    }

    @Test
    public void canCreateNodesInGremlinLand()
    {
        Response evaluatedGremlinResponse = consoleService.exec( "{ \"command\" : \"g:add-v()\" }" );

        assertEquals(200, evaluatedGremlinResponse.getStatus());
        assertThat((String)evaluatedGremlinResponse.getEntity(), containsString("v[1]"));
    }

    @Before
    public void setUp() throws Exception
    {
        ConsoleSession session = new ConsoleSession( new Database( new ImpermanentGraphDatabase( "target/tempdb" ) ) );
        this.consoleService = new ConsoleService( session );
    }

}
