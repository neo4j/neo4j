/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ImpermanentDbmsExtension
class ServerExecutionEngineTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldDetectPeriodicCommitQueries()
    {
        // GIVEN
        QueryExecutionEngine engine = db.getDependencyResolver()
                                          .resolveDependency( QueryExecutionEngine.class );

        // WHEN
        boolean result = engine.isPeriodicCommit("USING PERIODIC COMMIT LOAD CSV FROM 'file:///tmp/foo.csv' AS line CREATE ()");

        // THEN
        assertTrue( result, "Did not detect periodic commit query" );
    }

    @Test
    void shouldNotDetectNonPeriodicCommitQueriesAsPeriodicCommitQueries()
    {
        // GIVEN
        QueryExecutionEngine engine = db.getDependencyResolver()
                                          .resolveDependency( QueryExecutionEngine.class );

        // WHEN
        boolean result = engine.isPeriodicCommit("CREATE ()");

        // THEN
        assertFalse( result, "Did detect non-periodic commit query as periodic commit query" );
    }

    @Test
    void shouldNotDetectInvalidQueriesAsPeriodicCommitQueries()
    {
        // GIVEN
        QueryExecutionEngine engine = db.getDependencyResolver()
                                          .resolveDependency( QueryExecutionEngine.class );

        // WHEN
        boolean result = engine.isPeriodicCommit("MATCH n RETURN m");

        // THEN
        assertFalse( result, "Did detect an invalid query as periodic commit query" );
    }
}
