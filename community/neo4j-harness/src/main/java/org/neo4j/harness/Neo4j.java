/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.harness;

import java.io.PrintStream;
import java.net.URI;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;

/**
 * Neo4j test instance.
 */
@PublicApi
public interface Neo4j extends AutoCloseable
{
    /**
     * Returns the URI to the Bolt Protocol connector of the instance.
     * @return the bolt address.
     */
    URI boltURI();

    /**
     * Returns the URI to the root resource of the instance. For example, http://localhost:7474/
     * @return the http address to the root resource.
     */
    URI httpURI();

    /**
     * Returns ths URI to the root resource of the instance using the https protocol.
     * For example, https://localhost:7475/.
     * @return the https address to the root resource.
     */
    URI httpsURI();

    /**
     * Access the {@link DatabaseManagementService} used by the server.
     * @return the database management service backing this instance.
     */
    DatabaseManagementService databaseManagementService();

    /**
     * Access default database service.
     * @return default database service.
     */
    GraphDatabaseService defaultDatabaseService();

    /**
     * Returns the server's configuration.
     * @return the current configuration of the instance.
     */
    Configuration config();

    /**
     * Prints logs to the specified print stream if log is available.
     * @param out the stream to print to.
     */
    void printLogs( PrintStream out );

    /**
     * Shutdown neo4j test instance.
     */
    @Override
    void close();
}
