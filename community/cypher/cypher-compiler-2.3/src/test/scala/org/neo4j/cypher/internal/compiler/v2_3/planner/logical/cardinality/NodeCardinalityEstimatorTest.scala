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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import org.mockito.Mockito
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression, HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport2, Predicate, QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NodeCardinalityEstimatorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  import Mockito._

  val allNodes = Cardinality(1000)
  val inputCardinality = Cardinality(100)
  val qgInput = QueryGraphSolverInput(Map.empty, inputCardinality, None)

  val combiner = IndependenceCombiner
  val selectivityEstimator = mock[SelectivityEstimator]
  when( selectivityEstimator.combiner ).thenReturn( combiner )
  val estimator = NodeCardinalityEstimator(DelegatingSelectivityEstimator(selectivityEstimator), allNodes, inputCardinality)

  val hasPersonOnA: Expression = HasLabels(ident("a"), Seq(LabelName("Person")_))_
  val hasAnimalOnA: Expression = HasLabels(ident("a"), Seq(LabelName("Animal")_))_
  val hasPersonOnB: Expression = HasLabels(ident("b"), Seq(LabelName("Person")_))_

  val personSelectivity = Selectivity.of(0.5).get
  val animalSelectivity = Selectivity.of(0.1).get

  test("should estimate node labels") {
    when( selectivityEstimator.apply(hasPersonOnA ) ).thenReturn( personSelectivity )
    when( selectivityEstimator.apply(hasAnimalOnA ) ).thenReturn( animalSelectivity )
    when( selectivityEstimator.apply(hasPersonOnB ) ).thenReturn( personSelectivity )

    val qg = QueryGraph
      .empty
      .addPatternNodes("a")
      .addPatternNodes("b")
      .addPatternNodes("c")
      .addSelections(Selections(Set(
        Predicate(Set("a"), hasPersonOnA),
        Predicate(Set("a"), hasAnimalOnA),
        Predicate(Set("b"), hasPersonOnB)
    )))


    val (estimates, used) = estimator(qg)

    estimates should equal(Map(
      IdName("a") -> allNodes * personSelectivity * animalSelectivity,
      IdName("b") -> allNodes * personSelectivity,
      IdName("c") -> allNodes
    ))

    used should equal(Set(hasPersonOnA, hasAnimalOnA, hasPersonOnB))
  }

  test("should estimate arguments") {
    when( selectivityEstimator.apply(hasPersonOnA ) ).thenReturn( personSelectivity )
    when( selectivityEstimator.apply(hasAnimalOnA ) ).thenReturn( animalSelectivity )
    when( selectivityEstimator.apply(hasPersonOnB ) ).thenReturn( personSelectivity )

    val qg = QueryGraph
      .empty
      .addPatternNodes("a")
      .addArgumentIds(Seq("b"))
      .addSelections(Selections(Set(
        Predicate(Set("a"), hasPersonOnA),
        Predicate(Set("a"), hasAnimalOnA),
        Predicate(Set("b"), hasPersonOnB)
      )))


    val (estimates, used) = estimator(qg)

    estimates should equal(Map(
      IdName("a") -> allNodes * personSelectivity * animalSelectivity,
      IdName("b") -> inputCardinality * personSelectivity
    ))

    used should equal(Set(hasPersonOnA, hasAnimalOnA, hasPersonOnB))
  }

  // TODO: Discuss this
  test("should not use predicates that also apply to other nodes") {
    val pred1 = mock[Expression]
    val pred2 = mock[Expression]

    when( selectivityEstimator.apply( pred1 ) ).thenReturn( Selectivity.of( 0.5d ).get )
    when( selectivityEstimator.apply( pred2 ) ).thenReturn( Selectivity.of( 0.25d ).get )

    val qg = QueryGraph
      .empty
      .addPatternNodes("a")
      .addPatternNodes("b")
      .addSelections(Selections(Set(
        Predicate(Set("a"), pred1),
        Predicate(Set("a", "b"), pred2)
      )))


    val (estimates, used) = estimator(qg)

    estimates should equal(Map(
      IdName("a") -> allNodes * Selectivity.of( 0.5d ).get,
      IdName("b") -> allNodes
    ))

    used should equal(Set(pred1))
  }
}

