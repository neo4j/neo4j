/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class PatternRelationshipMultiplierCalculatorTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should return zero if there are no nodes with the given labels") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.EMPTY)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.EMPTY)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.EMPTY)

    val calculator = PatternRelationshipMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.relationshipMultiplier(relationship, Map("a" -> Set(labelName("L"))))

    result should equal(Multiplier.ZERO)
  }

  test("should not consider label selectivity twice") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.SINGLE)

    val calculator = PatternRelationshipMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.relationshipMultiplier(relationship, Map("a" -> Set(labelName("L"))))

    result should equal(Multiplier.ONE)
  }

  test("handles variable length paths over 32 in length") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality.SINGLE)

    val calculator = PatternRelationshipMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(33, Some(33)))

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    val result = calculator.relationshipMultiplier(relationship, Map("a" -> Set(labelName("L"))))

    // one node which has a single relationship to itself. Given the relationship uniqueness, we should get some result between 0 and 1, but not larger than 1
    result should be >= Multiplier.ZERO
    result should be <= Multiplier.ONE
  }

  test("should be able to produce multipliers larger than 1.0") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality.SINGLE)
    when(stats.nodesAllCardinality()).thenReturn(Cardinality(10))
    when(stats.patternStepCardinality(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternRelationshipMultiplierCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val labels = new mutable.HashMap[String, LabelId]()
    for (i <- 1 to 100) labels.put(i.toString, LabelId(i))
    val labelInfo = Map("a" -> labels.keys.map(labelName).toSet)

    implicit val semanticTable: SemanticTable = new SemanticTable(resolvedLabelNames = labels)
    val result = calculator.relationshipMultiplier(relationship, labelInfo)

    result should be >= Multiplier.ONE
  }
}
