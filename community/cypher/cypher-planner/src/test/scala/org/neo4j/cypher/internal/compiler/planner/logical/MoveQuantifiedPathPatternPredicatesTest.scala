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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MoveQuantifiedPathPatternPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport
    with AstConstructionTestSupport {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.GpmShortestPath
  )

  private def buildSinglePlannerQueryAndRewrite(query: String): SinglePlannerQuery = {
    val q = buildSinglePlannerQuery(query)
    q.endoRewrite(MoveQuantifiedPathPatternPredicates.rewriter)
  }

  // (start) ((a)-[r]->(b))+ (end)
  private val qpp = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"a", v"start"),
    rightBinding = NodeBinding(v"b", v"end"),
    patternRelationships =
      NonEmptyList(PatternRelationship(
        v"r",
        (v"a", v"b"),
        SemanticDirection.OUTGOING,
        Seq.empty,
        SimplePatternLength
      )),
    repetition = Repetition(min = 1, max = UpperBound.Unlimited),
    nodeVariableGroupings = Set(variableGrouping(v"a", v"a"), variableGrouping(v"b", v"b")),
    relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
  )

  test("should move QPP predicate with singleton variable") {
    val q = buildSinglePlannerQueryAndRewrite("MATCH (start) ((a)-[r]->(b) WHERE b.prop > 123)+ (end) RETURN 1 AS one")

    val pred = ForAllRepetitions(
      v"b",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      andedPropertyInequalities(greaterThan(prop("b", "prop"), literal(123)))
    )(pos)

    q.queryGraph.quantifiedPathPatterns shouldEqual Set(qpp)
    q.queryGraph.selections.flatPredicates should contain(pred)
  }

  test("should move QPP predicate with multiple singleton variable") {
    val q =
      buildSinglePlannerQueryAndRewrite("MATCH (start) ((a)-[r]->(b) WHERE b.prop > r.prop)+ (end) RETURN 1 AS one")

    val pred = ForAllRepetitions(
      v"b",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      andedPropertyInequalities(greaterThan(prop("b", "prop"), prop("r", "prop")))
    )(pos)

    q.queryGraph.quantifiedPathPatterns shouldEqual Set(qpp)
    q.queryGraph.selections.flatPredicates should contain(pred)
  }

  test("should move QPP predicate without dependencies") {
    val q = buildSinglePlannerQueryAndRewrite("MATCH (start) ((a)-[r]->(b) WHERE 123 > $param)+ (end) RETURN 1 AS one")

    val pred = ForAllRepetitions(
      v"a",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      greaterThan(literal(123), parameter("param", CTAny))
    )(pos)

    q.queryGraph.quantifiedPathPatterns shouldEqual Set(qpp)
    q.queryGraph.selections.flatPredicates should contain(pred)
  }

  test("should move QPP predicate with singleton variable within a Selective Path Pattern") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (start) ((a)-[r]->(b) WHERE b.prop > 123)+ (end) RETURN 1 AS one"
    )

    val pred = ForAllRepetitions(
      v"b",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      greaterThan(prop("b", "prop"), literal(123))
    )(pos)

    q.queryGraph.selectivePathPatterns.flatMap(_.allQuantifiedPathPatterns) shouldEqual Set(qpp)
    q.queryGraph.selectivePathPatterns.flatMap(_.selections.flatPredicates) should contain(pred)
  }

  test("should move QPP predicate with multiple singleton variable within a Selective Path Pattern") {
    val q =
      buildSinglePlannerQueryAndRewrite(
        "MATCH ANY SHORTEST (start) ((a)-[r]->(b) WHERE b.prop > r.prop)+ (end) RETURN 1 AS one"
      )

    val pred = ForAllRepetitions(
      v"b",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      greaterThan(prop("b", "prop"), prop("r", "prop"))
    )(pos)

    q.queryGraph.selectivePathPatterns.flatMap(_.allQuantifiedPathPatterns) shouldEqual Set(qpp)
    q.queryGraph.selectivePathPatterns.flatMap(_.selections.flatPredicates) should contain(pred)
  }

  test("should move QPP predicate without dependencies within a Selective Path Pattern") {
    val q = buildSinglePlannerQueryAndRewrite(
      "MATCH ANY SHORTEST (start) ((a)-[r]->(b) WHERE 123 > $param)+ (end) RETURN 1 AS one"
    )

    val pred = ForAllRepetitions(
      v"a",
      Set(variableGrouping("a", "a"), variableGrouping("b", "b"), variableGrouping("r", "r")),
      greaterThan(literal(123), parameter("param", CTAny))
    )(pos)

    q.queryGraph.selectivePathPatterns.flatMap(_.allQuantifiedPathPatterns) shouldEqual Set(qpp)
    q.queryGraph.selectivePathPatterns.flatMap(_.selections.flatPredicates) should contain(pred)
  }

  test("should keep the order of the node connections in the selective path pattern") {
    val query =
      """MATCH ANY SHORTEST (start)
        |                   ((a)-[r]->(b))+
        |                   (middle)-->(shmiddle)-->(quiddle)-->(doolittle)
        |                   ((c)-[r2]->(d))+
        |                   (end)-->(oneMore)-->(reallyTheEnd)-->(endFinal)-->(endFinal2)
        |RETURN 1 AS one""".stripMargin
    buildSinglePlannerQueryAndRewrite(query) should equal(
      buildSinglePlannerQuery(query)
    )
  }
}
