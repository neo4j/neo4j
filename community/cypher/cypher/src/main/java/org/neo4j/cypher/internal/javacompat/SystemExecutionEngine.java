/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CompilerLibrary;
import org.neo4j.cypher.internal.InternalCompilerLibrary;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.logging.InternalLogProvider;

/**
 * This class is a specialized form of the ExecutionEngine containing two internal Cypher engines.
 * The normal one inherited from the parent will be accessible from the outside and configured to only
 * accept a specialized subset of commands. The innerCypherExecutionEngine on the other hand will
 * understand the normal Cypher commands not available on the surface for the System Database.
 */
class SystemExecutionEngine extends ExecutionEngine {
    private org.neo4j.cypher.internal.ExecutionEngine innerCypherExecutionEngine; // doesn't understand ddl

    /**
     * Creates an execution engine around the given graph database wrapping an internal compiler factory for two level Cypher runtime.
     * This is used for processing system database commands, where the outer Cypher engine will only understand administration commands
     * and translate them into normal Cypher against the SYSTEM database, processed by the inner Cypher runtime, which understands normal Cypher.
     */
    SystemExecutionEngine(
            GraphDatabaseQueryService queryService,
            InternalLogProvider logProvider,
            CypherQueryCaches systemQueryCaches,
            CompilerFactory systemCompilerFactory,
            CypherQueryCaches normalQueryCaches,
            CompilerFactory normalCompilerFactory) {
        innerCypherExecutionEngine = makeExecutionEngine(
                queryService,
                normalQueryCaches,
                logProvider,
                new CompilerLibrary(normalCompilerFactory, this::normalExecutionEngine));
        cypherExecutionEngine = // only understands ddl
                makeExecutionEngine(
                        queryService,
                        systemQueryCaches,
                        logProvider,
                        new InternalCompilerLibrary(
                                systemCompilerFactory, this::normalExecutionEngine, this::outerExecutionEngine));
    }

    @Override
    public long clearQueryCaches() {
        return Math.max(innerCypherExecutionEngine.clearQueryCaches(), cypherExecutionEngine.clearQueryCaches());
    }

    org.neo4j.cypher.internal.ExecutionEngine normalExecutionEngine() {
        return innerCypherExecutionEngine;
    }

    private org.neo4j.cypher.internal.ExecutionEngine outerExecutionEngine() {
        return cypherExecutionEngine;
    }
}
