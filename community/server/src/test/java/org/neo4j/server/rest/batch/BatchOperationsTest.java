/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.batch;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;

import org.junit.Test;
import org.neo4j.server.rest.web.InternalJettyServletRequest;
import org.neo4j.server.rest.web.InternalJettyServletResponse;

public class BatchOperationsTest {

    private final BatchOperations ops = new BatchOperations(null) {
        @Override
        protected void invoke(String method, String path, String body, Integer id, URI targetUri, InternalJettyServletRequest req, InternalJettyServletResponse res) throws IOException, ServletException {
        }
    };

    @Test
    public void testReplaceLocations() throws Exception {
        Map<Integer,String> map=new HashMap<Integer, String>();
        map.put(100,"bar");
        assertEquals("foo", ops.replaceLocationPlaceholders("foo", map));
        assertEquals("foo bar", ops.replaceLocationPlaceholders("foo {100}", map));
        assertEquals("bar foo bar", ops.replaceLocationPlaceholders("{100} foo {100}", map));
        assertEquals("bar bar foo bar bar", ops.replaceLocationPlaceholders("bar {100} foo {100} bar", map));
    }
}
