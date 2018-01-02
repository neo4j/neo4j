/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.assumeIndependence

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength, VarPatternLength}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanConstructionTestSupport, Predicate, Selections}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.ast.{AstConstructionTestSupport, HasLabels, LabelName}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, SemanticDirection, SemanticTable}
import org.neo4j.graphdb.Direction

import scala.collection.mutable

class PatternSelectivityCalculatorTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  test("should return zero if there are no nodes with the given labels") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(0))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map(IdName("a") -> Set(label)))

    result should equal(Selectivity.ZERO)
  }

  test("should not consider label selectivity twice") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(1))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map(IdName("a") -> Set(label)))

    result should equal(Selectivity.ONE)
  }

  test("handles variable length paths over 32 in length") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(1))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(3))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength(33, Some(33)))

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map(IdName("a") -> Set(label)))

    result should equal(Selectivity.ONE)
  }

  test("should not produce selectivities larger than 1.0") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenAnswer(new Answer[Cardinality] {
      override def answer(invocationOnMock: InvocationOnMock): Cardinality = {
        val arg = invocationOnMock.getArguments()(0).asInstanceOf[Option[LabelId]]
        arg match {
          case None => Cardinality(10)
          case Some(_) => Cardinality(1)
        }
      }
    })
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    val labels = new mutable.HashMap[String, LabelId]()
    for (i <- 1 to 100) labels.put(i.toString, LabelId(i))
    val labelNames = labels.keys.map(LabelName(_)(pos))
    val predicates = labelNames.map(l => Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(l))(pos))).toSet

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = labels)
    implicit val selections = Selections(predicates)
    val result = calculator.apply(relationship, Map.empty)

    result should equal(Selectivity.ONE)
  }
}
