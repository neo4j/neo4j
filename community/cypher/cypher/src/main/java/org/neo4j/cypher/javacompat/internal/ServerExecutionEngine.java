/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.javacompat.internal;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * This is a variant of {@link ExecutionEngine} that provides additional
 * callbacks that are used by REST server's transactional endpoints for Cypher
 *
 * This is not public API
 */
public class ServerExecutionEngine extends ExecutionEngine
{
    private org.neo4j.cypher.internal.ServerExecutionEngine serverExecutionEngine;

    public ServerExecutionEngine( GraphDatabaseService database )
    {
        super( database );
    }

    public ServerExecutionEngine( GraphDatabaseService database, StringLogger logger )
    {
        super( database, logger );
    }

    @Override
    protected
    org.neo4j.cypher.ExecutionEngine createInnerEngine(GraphDatabaseService database, StringLogger logger)
    {
        serverExecutionEngine = new org.neo4j.cypher.internal.ServerExecutionEngine(database, logger);
        return serverExecutionEngine;
    }

    public boolean isPeriodicCommit( String query )
    {
        return serverExecutionEngine.isPeriodicCommit( query );
    }
}
