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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.test_helpers.TestGraphStatistics
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PatternRelationshipCardinalityModelTest extends CypherFunSuite with PatternRelationshipCardinalityModel {

  test("should return zero if there are no nodes with the given labels") {
    val graphStatistics = new TestGraphStatistics {
      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = Cardinality.EMPTY
    }
    val semanticTable = new SemanticTable(resolvedLabelNames = Map("L" -> LabelId(0)))
    val context = QueryGraphCardinalityContext(
      graphStatistics = graphStatistics,
      selectivityCalculator = null,
      combiner = IndependenceCombiner,
      relTypeInfo = null,
      semanticTable = semanticTable,
      indexPredicateProviderContext = null,
      cardinalityModel = null,
      allNodesCardinality = null,
      LabelInferenceStrategy.NoInference
    )
    val labelInfo: LabelInfo = Map(v"a" -> Set(LabelName("L")(InputPosition.NONE)))
    val cardinality = getSimpleRelationshipCardinality(
      context = context,
      labelInfo = labelInfo,
      leftNode = "a",
      rightNode = "b",
      relationshipTypes = Nil,
      relationshipDirection = SemanticDirection.OUTGOING
    )
    cardinality shouldEqual Cardinality.EMPTY
  }

  test("should not consider label selectivity twice") {
    val graphStatistics = new TestGraphStatistics {
      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = Cardinality.SINGLE
    }
    val semanticTable = new SemanticTable(resolvedLabelNames = Map("L" -> LabelId(0)))
    val context = QueryGraphCardinalityContext(
      graphStatistics = graphStatistics,
      selectivityCalculator = null,
      combiner = IndependenceCombiner,
      relTypeInfo = null,
      semanticTable = semanticTable,
      indexPredicateProviderContext = null,
      cardinalityModel = null,
      allNodesCardinality = null,
      LabelInferenceStrategy.NoInference
    )
    val labelInfo: LabelInfo = Map(v"a" -> Set(LabelName("L")(InputPosition.NONE)))
    val cardinality = getSimpleRelationshipCardinality(
      context = context,
      labelInfo = labelInfo,
      leftNode = "a",
      rightNode = "b",
      relationshipTypes = Nil,
      relationshipDirection = SemanticDirection.OUTGOING
    )
    cardinality shouldEqual Cardinality.SINGLE
  }

  test("handles variable length paths over 32 in length") {
    val graphStatistics = new TestGraphStatistics {
      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = Cardinality.SINGLE
    }
    val semanticTable = new SemanticTable(resolvedLabelNames = Map("L" -> LabelId(0)))
    val context = QueryGraphCardinalityContext(
      graphStatistics = graphStatistics,
      selectivityCalculator = null,
      combiner = IndependenceCombiner,
      relTypeInfo = null,
      semanticTable = semanticTable,
      indexPredicateProviderContext = null,
      cardinalityModel = null,
      allNodesCardinality = Cardinality.SINGLE,
      LabelInferenceStrategy.NoInference
    )
    val labelInfo: LabelInfo = Map(v"a" -> Set(LabelName("L")(InputPosition.NONE)))
    val relationship =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Nil, VarPatternLength(33, Some(33)))
    val cardinality = getRelationshipCardinality(context, labelInfo, relationship, isUnique = false)
    cardinality shouldEqual Cardinality.SINGLE
  }

  test("relationship cardinality if no relationship exist should be equal with/without existing token") {
    val graphStatistics = new MinimumGraphStatistics(new TestGraphStatistics {
      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = Cardinality.EMPTY
    })
    val semanticTable = new SemanticTable(resolvedRelTypeNames = Map("KNOWN" -> RelTypeId(0)))
    val context = QueryGraphCardinalityContext(
      graphStatistics = graphStatistics,
      selectivityCalculator = null,
      combiner = IndependenceCombiner,
      relTypeInfo = null,
      semanticTable = semanticTable,
      indexPredicateProviderContext = null,
      cardinalityModel = null,
      allNodesCardinality = null,
      LabelInferenceStrategy.NoInference
    )
    val directions = List(SemanticDirection.INCOMING, SemanticDirection.OUTGOING, SemanticDirection.BOTH)
    for (direction <- directions) withClue(direction) {
      val unknownRelCardinality = getSimpleRelationshipCardinality(
        context = context,
        labelInfo = Map.empty,
        leftNode = "a",
        rightNode = "b",
        relationshipTypes = List(RelTypeName("UNKNOWN")(InputPosition.NONE)),
        relationshipDirection = direction
      )
      val knownRelCardinality = getSimpleRelationshipCardinality(
        context = context,
        labelInfo = Map.empty,
        leftNode = "a",
        rightNode = "b",
        relationshipTypes = List(RelTypeName("KNOWN")(InputPosition.NONE)),
        relationshipDirection = direction
      )

      unknownRelCardinality shouldEqual knownRelCardinality
    }
  }
}
