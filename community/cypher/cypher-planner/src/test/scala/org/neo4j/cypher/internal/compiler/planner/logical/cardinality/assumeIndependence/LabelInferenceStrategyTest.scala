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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.test_helpers.TestGraphStatistics
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LabelInferenceStrategyTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should not infer labels for relationships with too many types") {
    val planContext = testPlanContext(
      mostCommonLabelGivenRelationshipTypeFunc = _ => Seq(1, 2, 3),
      labels = Seq(
        LabelDesc(id = 1, name = "A", cardinality = 200),
        LabelDesc(id = 2, name = "B", cardinality = 100),
        LabelDesc(id = 3, name = "C", cardinality = 300)
      )
    )

    val semanticTable = SemanticTable(
      resolvedRelTypeNames =
        Range.inclusive(0, LabelInferenceStrategy.REL_TYPE_LIMIT)
          .map(i => s"REL_$i" -> RelTypeId(123 + i))
          .toMap
    )

    val labelInference = LabelInferenceStrategy.fromConfig(planContext, CypherInferSchemaPartsOption.mostSelectiveLabel)

    def inferLabelsGivenRelTypes(relTypes: Seq[String]): LabelInfo = {
      val relTypeNames = relTypes.map(relTypeName(_))
      val patternRel = PatternRelationship(
        v"r",
        (v"n", v"m"),
        SemanticDirection.OUTGOING,
        relTypeNames,
        SimplePatternLength
      )
      val (inferredLabelInfo, _) = labelInference.inferLabels(
        semanticTable,
        planContext.statistics,
        LabelInfo.empty,
        Seq(patternRel)
      )
      inferredLabelInfo
    }

    val relTypesWithinLimit =
      semanticTable.resolvedRelTypeNames.keySet.take(LabelInferenceStrategy.REL_TYPE_LIMIT).toSeq
    inferLabelsGivenRelTypes(relTypesWithinLimit) shouldBe LabelInfo(
      v"n" -> Set(labelName("B")),
      v"m" -> Set(labelName("B"))
    )

    val relTypesAboveLimit = semanticTable.resolvedRelTypeNames.keySet.toSeq
    inferLabelsGivenRelTypes(relTypesAboveLimit) shouldBe LabelInfo.empty
  }

  private case class LabelDesc(id: Int, name: String, cardinality: Int)

  private def testPlanContext(
    mostCommonLabelGivenRelationshipTypeFunc: Int => Seq[Int],
    labels: Seq[LabelDesc]
  ): PlanContext = {

    val testStatistics = new TestGraphStatistics {
      override def mostCommonLabelGivenRelationshipType(typ: Int): Seq[Int] =
        mostCommonLabelGivenRelationshipTypeFunc(typ)

      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = {
        Cardinality(123)
      }

      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = Cardinality {
        labelId.fold(labels.map(_.cardinality).sum) { labelId =>
          labels
            .find(_.id == labelId.id)
            .getOrElse(throw new IllegalArgumentException(s"Label not found, id = $labelId"))
            .cardinality
        }
      }
    }

    new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics =
        InstrumentedGraphStatistics(testStatistics, new MutableGraphStatisticsSnapshot())

      override def getLabelName(id: Int): String = {
        labels
          .find(_.id == id)
          .getOrElse(throw new IllegalArgumentException(s"Label not found, id = $id"))
          .name
      }
    }
  }
}
