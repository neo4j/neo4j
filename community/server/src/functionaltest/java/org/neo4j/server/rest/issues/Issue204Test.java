/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.issues;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.web.CypherService;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import javax.ws.rs.core.Response.Status;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static javax.ws.rs.core.Response.Status.*;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

public class Issue204Test extends AbstractRestFunctionalTestBase {

    
    /**
     * A simple query returning all nodes connected to node 1, returning the
     * node and the name property, if it exists, otherwise `null`:
     */
    @Test
    @Documented
    @Title( "Send a Query" )
    @Graph( "I know you" )
    public void testPropertyColumn() throws UnsupportedEncodingException {
                     data.get();
        String response = gen().expectedStatus(OK.getStatusCode()).payload("{\"command\":\"g.clear()\",\"engine\":\"gremlin\"}").post("http://localhost:7474/db/manage/server/console/").entity();

        response = gen().expectedStatus(OK.getStatusCode()).payload("{\"command\":\"g.addVertex([name:'foo'])\",\"engine\":\"gremlin\"}").post("http://localhost:7474/db/manage/server/console/").entity();
        assertThat( response, containsString( "you" ) );
        assertThat( response, containsString( "him" ) );
        assertThat( response, containsString( "25" ) );
        assertThat( response, not( containsString( "\"x\"" ) ) );
    }
    

}
