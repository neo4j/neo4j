/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.LabelId
import org.neo4j.cypher.internal.compiler.v2_2.ast.{AstConstructionTestSupport, HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanConstructionTestSupport, Predicate, Selections, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.graphdb.Direction

import scala.collection.mutable

class PatternSelectivityCalculatorTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  test("should not divide by zero if there are no node with a given label") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(0))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map(IdName("a") -> Set(label)))

    result should equal(Selectivity.ONE)
  }

  test("should not consider label selectivity twice") {
    val stats: GraphStatistics = mock[GraphStatistics]
    when(stats.nodesWithLabelCardinality(any())).thenReturn(Cardinality(1))
    when(stats.cardinalityByLabelsAndRelationshipType(any(), any(), any())).thenReturn(Cardinality(42))

    val calculator = PatternSelectivityCalculator(stats, IndependenceCombiner)
    val relationship = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

    val label = LabelName("L")(pos)

    implicit val semanticTable = new SemanticTable(resolvedLabelIds = mutable.Map("L" -> LabelId(0)))
    implicit val selections = Selections(Set(Predicate(Set[IdName]("a"), HasLabels(ident("a"), Seq(label))(pos))))
    val result = calculator.apply(relationship, Map(IdName("a") -> Set(label)))

    result should equal(Selectivity(42))
  }
}
