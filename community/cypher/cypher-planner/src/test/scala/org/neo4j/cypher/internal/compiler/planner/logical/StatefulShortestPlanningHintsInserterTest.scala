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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathAll
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathInto
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatefulShortestPlanningHintsInserterTest extends CypherFunSuite with LogicalPlanningTestSupport
    with AstConstructionTestSupport {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.MatchModes
  )

  private def buildSinglePlannerQueryAndRewrite(
    query: String,
    statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption
  ): SinglePlannerQuery = {
    val context = ContextHelper.create(
      statefulShortestPlanningMode = statefulShortestPlanningMode
    )

    val q = buildSinglePlannerQuery(query)
    q.endoRewrite(StatefulShortestPlanningHintsInserter.instance(mock[LogicalPlanState], context))
  }

  test("should insert no hint if using cardinality_heuristic") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (a)-[r]->*(b) RETURN *",
      CypherStatefulShortestPlanningModeOption.cardinalityHeuristic
    )

    q.allHints should be(empty)
  }

  test("should insert a hint if using into_only") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) RETURN *",
      CypherStatefulShortestPlanningModeOption.intoOnly
    )

    q.allHints should be(Set(UsingStatefulShortestPathInto(NonEmptyList(v"a", v"b", v"r", v"c", v"d"))))
  }

  test("should insert a hint if using all_if_possible") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) RETURN *",
      CypherStatefulShortestPlanningModeOption.allIfPossible
    )

    q.allHints should be(Set(UsingStatefulShortestPathAll(NonEmptyList(v"a", v"b", v"r", v"c", v"d"))))
  }

  test("should not insert a hint if start == end, if using all_if_possible") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (a) RETURN *",
      CypherStatefulShortestPlanningModeOption.allIfPossible
    )

    q.allHints should be(empty)
  }

  test("should not insert a hint if start and end previously bound, if using all_if_possible") {
    val q = buildSinglePlannerQueryAndRewrite(
      """
        |MATCH (a), (d)
        |WITH * SKIP 10
        |MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) 
        |RETURN *""".stripMargin,
      CypherStatefulShortestPlanningModeOption.allIfPossible
    )

    q.allHints should be(empty)
  }

  test("should insert a hint if only start previously bound, if using all_if_possible") {
    val q = buildSinglePlannerQueryAndRewrite(
      """
        |MATCH (a)
        |WITH * SKIP 10
        |MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) 
        |RETURN *""".stripMargin,
      CypherStatefulShortestPlanningModeOption.allIfPossible
    )

    q.allHints should be(Set(UsingStatefulShortestPathAll(NonEmptyList(v"a", v"b", v"r", v"c", v"d"))))
  }

  test("should insert multiple hints if multiple SSPs in a MATCH") {
    val q = buildSinglePlannerQueryAndRewrite(
      """MATCH REPEATABLE ELEMENTS 
        |  ANY SHORTEST (a) ((b)-[r]->(c))* (d), 
        |  ANY SHORTEST (a2) ((b2)-[r2]->(c2))* (d2) 
        |RETURN *""".stripMargin,
      CypherStatefulShortestPlanningModeOption.intoOnly
    )

    q.allHints should be(Set(
      UsingStatefulShortestPathInto(NonEmptyList(v"a", v"b", v"r", v"c", v"d")),
      UsingStatefulShortestPathInto(NonEmptyList(v"a2", v"b2", v"r2", v"c2", v"d2"))
    ))
  }

}
