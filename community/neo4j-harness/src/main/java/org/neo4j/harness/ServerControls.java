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
package org.neo4j.harness;

import java.net.URI;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Control panel for a Neo4j test instance.
 */
public interface ServerControls extends AutoCloseable
{
    /** Returns the URI to the root resource of the instance. For example, http://localhost:7474/ */
    URI httpURI();

    /**
     * Returns ths URI to the root resource of the instance using the https protocol.
     * For example, https://localhost:7475/.
     */
    URI httpsURI();

    /** Stop the test instance and delete all files related to it on disk. */
    @Override
    void close();

    /** Access the {@link org.neo4j.graphdb.GraphDatabaseService} used by the server */
    GraphDatabaseService graph();
}
