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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.test_helpers.TestGraphStatistics
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class QuantifiedPathPatternCardinalityModelTest extends CypherFunSuite with QuantifiedPathPatternCardinalityModel {

  test("should calculate uniqueness selectivity correctly for QPPs") {
    val graphStatistics = new TestGraphStatistics {
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
        labelId match {
          case Some(LabelId(0)) => Cardinality(50)
          case Some(LabelId(1)) => Cardinality(20)
          case _                => sys.error(s"missing node cardinality: $labelId")
        }

      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality =
        (fromLabel, relTypeId, toLabel) match {
          case (None, Some(RelTypeId(0)), None)             => Cardinality(40)
          case (Some(LabelId(0)), Some(RelTypeId(0)), None) => Cardinality(35)
          case (Some(LabelId(1)), Some(RelTypeId(0)), None) => Cardinality(15)
          case _ => sys.error(s"missing rel cardinality: (:$fromLabel)-[:$relTypeId]->(:$toLabel)")
        }
    }

    val semanticTable =
      new SemanticTable(
        resolvedLabelNames = mutable.Map("A" -> LabelId(0), "B" -> LabelId(1)),
        resolvedRelTypeNames = mutable.Map("R" -> RelTypeId(0))
      )

    val context = QueryGraphCardinalityContext(
      graphStatistics = graphStatistics,
      selectivityCalculator = null,
      combiner = IndependenceCombiner,
      relTypeInfo = Map.empty,
      semanticTable = semanticTable,
      indexPredicateProviderContext = null,
      cardinalityModel = null,
      allNodesCardinality = Cardinality(200)
    )

    val labelInfo =
      Map(
        "start" -> Set(LabelName("A")(InputPosition.NONE)),
        "end" -> Set(LabelName("B")(InputPosition.NONE))
      )

    val qpp =
      QuantifiedPathPattern(
        leftBinding = NodeBinding("a_i", "start"),
        rightBinding = NodeBinding("c_i", "end"),
        patternRelationships = NonEmptyList(
          PatternRelationship(
            "r_i",
            ("a_i", "b_i"),
            SemanticDirection.OUTGOING,
            List(RelTypeName("R")(InputPosition.NONE)),
            SimplePatternLength
          ),
          PatternRelationship(
            "s_i",
            ("b_i", "c_i"),
            SemanticDirection.INCOMING,
            List(RelTypeName("R")(InputPosition.NONE)),
            SimplePatternLength
          )
        ),
        selections = Selections.from(DifferentRelationships(
          Variable("r_i")(InputPosition.NONE),
          Variable("s_i")(InputPosition.NONE)
        )(InputPosition.NONE)),
        repetition = Repetition.apply(2, UpperBound.Limited(2)),
        nodeVariableGroupings =
          Set(VariableGrouping("a_i", "a"), VariableGrouping("b_i", "b"), VariableGrouping("c_i", "c")),
        relationshipVariableGroupings = Set(VariableGrouping("r_i", "r"), VariableGrouping("s_i", "s"))
      )

    val cardinality =
      getQuantifiedPathPatternCardinality(context, labelInfo, quantifiedPathPattern = qpp, Set("r", "s"))

    // (:A)(()-[r:R]->()<-[s:R]-() WHERE r <> s){2}(end:B)
    cardinality shouldEqual Cardinality(
      35.0 * 40.0 / 200.0 * 40.0 / 200.0 * 15.0 / 200.0 * // (:A)-[r1:R]->()<-[s1:R]-()-[r2:R]->()<-[s2:R]-()
        math.pow(.99, 6) // r1 <> s1 AND r1 <> r2 AND r1 <> s2 AND s1 <> r2 AND s1 <> s2 AND r2 <> s2
    )
  }
}
