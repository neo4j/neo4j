/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.kernel.monitoring.Monitors
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.NewQueryPlanSuccessRateMonitor
import org.neo4j.cypher.internal.compiler.v2_1.{AstRewritingMonitor, SemanticCheckMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.parser.ParserMonitor

class CypherCompilerTest extends CypherFunSuite {

  test("isPeriodicCommit handles versioned queries") {
    val gds = mock[GraphDatabaseService]

    val monitors: Monitors = mock[Monitors]
    when(monitors.newMonitor(classOf[NewQueryPlanSuccessRateMonitor], "compiler2.1")).thenReturn(mock[NewQueryPlanSuccessRateMonitor])
    when(monitors.newMonitor(classOf[SemanticCheckMonitor])).thenReturn(mock[SemanticCheckMonitor])
    when(monitors.newMonitor(classOf[AstRewritingMonitor])).thenReturn(mock[AstRewritingMonitor])
    when(monitors.newMonitor(classOf[ParserMonitor], "compiler2.1")).thenReturn(mock[ParserMonitor])

    val compiler = new CypherCompiler(gds, monitors)
    compiler.isPeriodicCommit("CYPHER 2.1 USING PERIODIC COMMIT LOAD CSV FROM 'file:///tmp/foo.csv' AS line CREATE ()") should equal(true)
  }
}
