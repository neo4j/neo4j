/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest;

import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.GraphDescription.Graph;

import static org.junit.Assert.assertEquals;

public class ConsoleDocTest extends AbstractRestFunctionalTestBase
{
    /**
     * Paths can be returned
     * together with other return types by just
     * specifying returns.
     */
    @Test
    @Documented
    @Graph( "I know you" )
    public void testShell() throws Exception {
        String command = "ls";
        String response = shellCall(command, Status.OK);

        assertEquals( 2, ( JsonHelper.jsonToList( response ) ).size() );
    }


    private String shellCall(String command, Status status,
                             Pair<String, String>... params)
    {
        return gen().payload("{\"command\":\""+command+"\",\"engine\":\"shell\"}").expectedStatus(status.getStatusCode()).post(consoleUri() ).entity();
    }
    

    private String consoleUri()
    {
        return getDatabaseUri() + "manage/server/console";
    }

    
}
