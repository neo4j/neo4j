/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.plannerQuery.StatementConverters.toUnionQuery
import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.frontend.v3_2.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Query
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.flattenBooleanOperators
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{DummyPosition, SemanticChecker, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_2._

class OptionalMatchReorganizerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val rewriter = OptionalMatchReorganizer.instance(null)

  test("should move one optional match to after query horizon") {
    /*
    MATCH (a)
    OPTIONAL MATCH (b)
    OPTIONAL MATCH (c)
     */
    val optional1 = QueryGraph.empty.withPatternNodes(Set(IdName("b")))
    val optional2 = QueryGraph.empty.withPatternNodes(Set(IdName("c")))
    val root = QueryGraph.empty.
      withPatternNodes(Set(IdName("a"))).
      withAddedOptionalMatch(optional1).
      withAddedOptionalMatch(optional2)
    val projection = RegularQueryProjection(Map("a" -> varFor("a")))
    val pq = RegularPlannerQuery(root, projection)

    val expectedQG1 = QueryGraph.empty.
      withPatternNodes(Set(IdName("a"))).
      withAddedOptionalMatch(optional1)

    val expectedQG2 = QueryGraph.empty.
      withArgumentIds(Set(IdName("a"))).
      withAddedOptionalMatch(optional2)

    val tail = RegularPlannerQuery(expectedQG2, projection)
    val expected = RegularPlannerQuery(expectedQG1, PassthroughAllHorizon(), Some(tail))

    val query = pq.endoRewrite(rewriter)
    query should equal(expected)
  }

  private def parseForRewriting(queryText: String) = parser.parse(queryText.replace("\r\n", "\n"))
}
