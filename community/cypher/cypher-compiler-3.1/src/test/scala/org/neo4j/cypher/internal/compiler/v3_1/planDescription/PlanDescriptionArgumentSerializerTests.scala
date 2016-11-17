/*
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
package org.neo4j.cypher.internal.compiler.v3_1.planDescription

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.ProjectedPath.{nilProjector, singleNodeProjector}
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{NestedPipeExpression, ProjectedPath}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.{ArgumentPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class PlanDescriptionArgumentSerializerTests extends CypherFunSuite {

  test("serialization should leave numeric arguments as numbers") {
    serialize(DbHits(12)) shouldBe a [java.lang.Number]
    serialize(Rows(12)) shouldBe a [java.lang.Number]
    serialize(EstimatedRows(12)) shouldBe a [java.lang.Number]
  }

  test("ExpandExpression should look like Cypher syntax") {
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(1))) should
      equal("(a)-[r:LIKES|:LOVES]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, Some(5))) should
      equal("(a)-[r:LIKES|:LOVES*..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 1, None)) should
      equal("(a)-[r:LIKES|:LOVES*]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, Some(5))) should
      equal("(a)-[r:LIKES|:LOVES*3..5]->(b)")
    serialize(ExpandExpression("a", "r", Seq("LIKES", "LOVES"), "b", SemanticDirection.OUTGOING, 3, None)) should
      equal("(a)-[r:LIKES|:LOVES*3..]->(b)")
  }

  test("serialize nested pipe expression") {
    val nested = NestedPipeExpression(ArgumentPipe(SymbolTable())(None)(mock[PipeMonitor]), ProjectedPath(
      Set("a"),
      singleNodeProjector("a", nilProjector)
    ))

    serialize(LegacyExpression(nested)) should equal("NestedExpression(Argument)")
  }
}
