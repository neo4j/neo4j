/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin.webtest;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.rest.domain.GraphDbHelper;

public class IntegrationTestHelper {
    private final NeoServerWithEmbeddedWebServer server;
    private final GraphDbHelper helper;

    public IntegrationTestHelper(NeoServerWithEmbeddedWebServer server) {
        if (server.getDatabase() == null) {
            throw new RuntimeException("Server must be started before using " + getClass().getName());
        }
        this.helper = new GraphDbHelper(server.getDatabase());
        this.server = server;
    }

    public GraphDbHelper getGraphDbHelper() {
        return helper;
    }


    public String dataUri() {
        return server.baseUri().toString() + "db/data/";
    }

    String nodeUri() {
        return dataUri() + "node";
    }

    public String nodeUri(long id) {
        return nodeUri() + "/" + id;
    }

    public AbstractGraphDatabase getDatabase() {
        return server.getDatabase().graph;
    }
}
