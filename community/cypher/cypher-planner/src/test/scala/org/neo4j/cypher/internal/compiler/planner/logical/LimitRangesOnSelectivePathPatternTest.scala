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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LimitRangesOnSelectivePathPatternTest extends CypherFunSuite with LogicalPlanningTestSupport
    with AstConstructionTestSupport {

  test("should move QPP max limit from QPP to SPP predicate when above 100") {
    val rewrittenSpp =
      buildSinglePlannerQueryAndRewriteSelectivePathPattern(
        "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){1,1000} (end)) RETURN 1 AS one"
      )
    val spp =
      getSPPWithLimits(Seq((1, UpperBound.unlimited)), Seq(maxRestrictionPredicate(v"r", "1000")))
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP max limit from QPP to SPP predicate when below 100") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){1,8} (end)) RETURN 1 AS one"
    )

    val spp = getSPPWithLimits(Seq((1, UpperBound.Limited(8))), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP max limit from QPP to SPP predicate with no upper limit") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b))+ (end)) RETURN 1 AS one"
    )
    val spp = getSPPWithLimits(Seq((1, UpperBound.unlimited)), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should move QPP min limit from QPP to SPP predicate when above 100") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){101,} (end)) RETURN 1 AS one"
    )
    val spp = getSPPWithLimits(Seq((1, UpperBound.unlimited)), Seq(minRestrictionPredicate(v"r", "101")))
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP min limit from QPP to SPP predicate when below 100") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){8,} (end)) RETURN 1 AS one"
    )

    val spp = getSPPWithLimits(Seq((8, UpperBound.unlimited)), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP min limit from QPP to SPP predicate with 0 lower limit") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b))* (end)) RETURN 1 AS one"
    )
    val spp = getSPPWithLimits(Seq((0, UpperBound.unlimited)), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should move QPP (2 rels) min limit from QPP to SPP predicate when above 50") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)-[r2]->(c)){1,51} (end)) RETURN 1 AS one"
    )
    val spp = get2RelsSPPWithLimits(1, UpperBound.unlimited, Seq(maxRestrictionPredicate(v"r", "51")))
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP (2 rels) min limit from QPP to SPP predicate when below 50") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)-[r2]->(c)){1,49} (end)) RETURN 1 AS one"
    )
    val spp = get2RelsSPPWithLimits(1, UpperBound.Limited(49), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should move QPP limit from multiple QPPs to SPP predicate when above 100") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){1,100} (end) ((c)-[s]->(d)){1,100} (end2)) RETURN 1 AS one"
    )
    val spp = getSPPWithLimits(
      Seq((1, UpperBound.unlimited), (1, UpperBound.unlimited)),
      Seq(maxRestrictionPredicate(v"r", "100"), maxRestrictionPredicate(v"s", "100"))
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test(
    "should move QPP limit from multiple QPPs to SPP predicate but not set a path limit if query contains an unlimited UpperBound"
  ) {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b))+ (end) ((c)-[s]->(d)){1,100} (end2)) RETURN 1 AS one"
    )

    val spp = getSPPWithLimits(
      Seq((1, UpperBound.unlimited), (1, UpperBound.unlimited)),
      Seq(maxRestrictionPredicate(v"s", "100"))
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test(
    "should move varLength with large lower and upper bound limit to SPP predicate"
  ) {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST (a)-[r*101..1001]->(b) RETURN 1 AS one"
    )
    val varLengthPattern =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(1, None))
    val spp = SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList.from(Seq(varLengthPattern))),
      Selections.from(Seq(
        Unique(v"r")(pos),
        minRestrictionPredicate(v"r", "101"),
        maxRestrictionPredicate(v"r", "1001"),
        VarLengthLowerBound(v"r", 101L)(pos),
        VarLengthUpperBound(v"r", 1001L)(pos)
      )),
      SelectivePathPattern.Selector.Shortest(1)
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test(
    "should move varLength with large lower bound limit to SPP predicate"
  ) {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST (a)-[r*101..]->(b) RETURN 1 AS one"
    )
    val varLengthPattern =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(1, None))
    val spp = SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList.from(Seq(varLengthPattern))),
      Selections.from(Seq(
        Unique(v"r")(pos),
        minRestrictionPredicate(v"r", "101"),
        VarLengthLowerBound(v"r", 101L)(pos)
      )),
      SelectivePathPattern.Selector.Shortest(1)
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test(
    "should move varLength with large upper bound limit to SPP predicate"
  ) {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST (a)-[r*..1001]->(b) RETURN 1 AS one"
    )
    val varLengthPattern =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(1, None))
    val spp = SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList.from(Seq(varLengthPattern))),
      Selections.from(Seq(
        Unique(v"r")(pos),
        maxRestrictionPredicate(v"r", "1001"),
        VarLengthLowerBound(v"r", 1L)(pos),
        VarLengthUpperBound(v"r", 1001L)(pos)
      )),
      SelectivePathPattern.Selector.Shortest(1)
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test(
    "should not move varLength with small lower and upper bound limit to SPP predicate"
  ) {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST (a)-[r*1..8]->(b) RETURN 1 AS one"
    )
    val varLengthPattern =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(1, Some(8)))
    val spp = SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList.from(Seq(varLengthPattern))),
      Selections.from(Seq(Unique(v"r")(pos), VarLengthLowerBound(v"r", 1L)(pos), VarLengthUpperBound(v"r", 8L)(pos))),
      SelectivePathPattern.Selector.Shortest(1)
    )
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should move QPP max limit from QPP to SPP predicate when above configured limit") {
    val rewrittenSpp =
      buildSinglePlannerQueryAndRewriteSelectivePathPattern(
        "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){1,1000} (end)) RETURN 1 AS one",
        limit = 999
      )
    val spp =
      getSPPWithLimits(Seq((1, UpperBound.unlimited)), Seq(maxRestrictionPredicate(v"r", "1000")))
    rewrittenSpp shouldEqual Set(spp)
  }

  test("should not move QPP max limit from QPP to SPP predicate when below configured limit") {
    val rewrittenSpp = buildSinglePlannerQueryAndRewriteSelectivePathPattern(
      "MATCH ANY SHORTEST ((start) ((a)-[r]->(b)){1,1000} (end)) RETURN 1 AS one",
      limit = 1001
    )

    val spp = getSPPWithLimits(Seq((1, UpperBound.Limited(1000))), Seq.empty)
    rewrittenSpp shouldEqual Set(spp)
  }

  private def buildSinglePlannerQueryAndRewriteSelectivePathPattern(
    query: String,
    limit: Int = 100
  ): Set[SelectivePathPattern] = {
    val q = buildSinglePlannerQuery(query)
    q.queryGraph.selectivePathPatterns.map(LimitRangesOnSelectivePathPattern(limit).apply)
  }

  private def getQPP1WithLimits(qppMin: Int, qppMax: UpperBound): QuantifiedPathPattern =
    QuantifiedPathPattern(
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
      repetition = Repetition(min = qppMin, max = qppMax),
      nodeVariableGroupings = Set(variableGrouping(v"a", v"a"), variableGrouping(v"b", v"b")),
      relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
    )

  private def get2RelsQPP1WithLimits(qppMin: Int, qppMax: UpperBound): QuantifiedPathPattern =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(v"a", v"start"),
      rightBinding = NodeBinding(v"c", v"end"),
      patternRelationships =
        NonEmptyList(
          PatternRelationship(
            v"r",
            (v"a", v"b"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          ),
          PatternRelationship(
            v"r2",
            (v"b", v"c"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          )
        ),
      repetition = Repetition(min = qppMin, max = qppMax),
      nodeVariableGroupings =
        Set(variableGrouping(v"a", v"a"), variableGrouping(v"b", v"b"), variableGrouping(v"c", v"c")),
      relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"), variableGrouping(v"r2", v"r2")),
      selections = Selections.from(differentRelationships("r2", "r"))
    )

  private def getQPP2WithLimits(qppMin: Int, qppMax: UpperBound): QuantifiedPathPattern =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(v"c", v"end"),
      rightBinding = NodeBinding(v"d", v"end2"),
      patternRelationships =
        NonEmptyList(PatternRelationship(
          v"s",
          (v"c", v"d"),
          SemanticDirection.OUTGOING,
          Seq.empty,
          SimplePatternLength
        )),
      repetition = Repetition(min = qppMin, max = qppMax),
      nodeVariableGroupings = Set(variableGrouping(v"c", v"c"), variableGrouping(v"d", v"d")),
      relationshipVariableGroupings = Set(variableGrouping(v"s", v"s"))
    )

  private def getSPPWithLimits(
    qppLimits: Seq[(Int, UpperBound)],
    predicates: Seq[Expression]
  ): SelectivePathPattern = {
    val qpps = qppLimits match {
      case Seq((min, max))                 => Seq(getQPP1WithLimits(min, max))
      case Seq((min1, max1), (min2, max2)) => Seq(getQPP1WithLimits(min1, max1), getQPP2WithLimits(min2, max2))
      case _                               => throw new IllegalStateException()
    }
    val relUniqPreds = if (qppLimits.size == 2) Seq(Disjoint(v"r", v"s")(pos), Unique(v"r")(pos), Unique(v"s")(pos))
    else Seq(Unique(v"r")(pos))
    SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList.from(qpps)),
      Selections.from(relUniqPreds ++ predicates),
      SelectivePathPattern.Selector.Shortest(1)
    )
  }

  private def get2RelsSPPWithLimits(
    qppMin: Int,
    qppMax: UpperBound,
    predicates: Seq[Expression]
  ): SelectivePathPattern = {
    val qpp = get2RelsQPP1WithLimits(qppMin, qppMax)
    val relUniqPreds = Seq(Unique(add(v"r", v"r2"))(pos))
    SelectivePathPattern(
      pathPattern = NodeConnections(NonEmptyList(qpp)),
      Selections.from(relUniqPreds ++ predicates),
      SelectivePathPattern.Selector.Shortest(1)
    )
  }

  private def minRestrictionPredicate(variable: LogicalVariable, min: String): GreaterThanOrEqual =
    GreaterThanOrEqual(
      FunctionInvocation(FunctionName("size")(pos), variable)(pos),
      SignedDecimalIntegerLiteral(min)(pos)
    )(pos)

  private def maxRestrictionPredicate(variable: LogicalVariable, max: String): LessThanOrEqual =
    LessThanOrEqual(
      FunctionInvocation(FunctionName("size")(pos), variable)(pos),
      SignedDecimalIntegerLiteral(max)(pos)
    )(pos)
}
