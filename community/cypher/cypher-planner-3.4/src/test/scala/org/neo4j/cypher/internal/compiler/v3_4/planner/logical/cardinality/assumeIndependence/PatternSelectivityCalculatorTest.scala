/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.cardinality.assumeIndependence

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.{VarPatternLength, _}
import org.neo4j.cypher.internal.planner.v3_4.spi.GraphStatistics
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, LabelId, Selectivity}
import org.neo4j.cypher.internal.v3_4.expressions.{HasLabels, LabelName, SemanticDirection}

import scala.collection.mutable

class PatternSelectivityCalculatorTest extends CypherFunSuite  with AstConstructionTestSupport {

  test("should return zero if there are no nodes with the given labels") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(0))
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.EMPTY)
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[String]("a"), HasLabels(varFor("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map("a" -> Set(label)))

    result should equal(Selectivity.ZERO)
  }

  test("should not consider label selectivity twice") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(1))
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[String]("a"), HasLabels(varFor("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map("a" -> Set(label)))

    result should equal(Selectivity.ONE)
  }

  test("handles variable length paths over 32 in length") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(1))
    when(stats.nodesAllCardinality()).thenReturn(Cardinality.SINGLE)
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(3))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(33, Some(33)))

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelNames = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[String]("a"), HasLabels(varFor("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map("a" -> Set(label)))

    result should equal(Selectivity.ONE)
  }

  test("should not produce selectivities larger than 1.0") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenAnswer(new Answer[Cardinality] {
      override def answer(invocationOnMock: InvocationOnMock): Cardinality = {
        val arg:Option[LabelId] = invocationOnMock.getArgument(0)
        arg match {
          case None => Cardinality(1)
          case Some(_) => Cardinality(1)
        }
      }
    })
    when(stats.nodesAllCardinality()).thenReturn(Cardinality(10))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val labels = new mutable.HashMap[String, LabelId]()
    for (i <- 1 to 100) labels.put(i.toString, LabelId(i))
    val labelNames = labels.keys.map(LabelName(_)(pos))
    val predicates = labelNames.map(l => Predicate(Set[String]("a"), HasLabels(varFor("a"), Seq(l))(pos))).toSet

    implicit val semanticTable = new SemanticTable(resolvedLabelNames = labels)
    implicit val selections = Selections(predicates)
    val result = calculator.apply(relationship, Map.empty)

    result should equal(Selectivity.ONE)
  }
}
