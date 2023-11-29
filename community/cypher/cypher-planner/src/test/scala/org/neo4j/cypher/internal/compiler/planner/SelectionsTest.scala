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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val aIsPerson = hasLabels("a", "Person")
  private val aIsProgrammer = hasLabels("a", "Programmer")
  private val bIsAnimal = hasLabels("b", "Animal")

  private val aId = idNames("a")
  private val bId = idNames("b")

  test("can flat predicates to a sequence") {
    val selections = Selections(Set(Predicate(aId, aIsPerson)))

    selections.flatPredicates should equal(Seq(aIsPerson))
  }

  test("can flat empty predicates to an empty sequence") {
    Selections().flatPredicates should equal(Seq())
  }

  test("should be able to sense that predicates are not covered") {
    val selections = Selections(Set(
      Predicate(aId, aIsPerson),
      Predicate(bId, bIsAnimal)
    ))

    selections.coveredBy(Seq(aIsPerson)) should be(right = false)
  }

  test("should be able to tell when all predicates are covered") {
    val selections = Selections(Set(
      Predicate(aId, aIsPerson)
    ))

    selections.coveredBy(Seq(aIsPerson)) should be(right = true)
  }

  test("can extract HasLabels Predicates") {
    val selections = Selections(Set(
      Predicate(aId, aIsPerson),
      Predicate(aId, aIsPerson),
      Predicate(bId, bIsAnimal),
      Predicate(idNames("c"), equals(varFor("c"), literalInt(42)))
    ))

    selections.labelPredicates should equal(Map(
      v"a" -> Set(aIsPerson),
      v"b" -> Set(bIsAnimal)
    ))
  }

  test("can find predicates given covered ids") {
    val selections = Selections(Set(
      Predicate(aId, aIsPerson),
      Predicate(bId, bIsAnimal)
    ))

    selections.predicatesGiven(aId) should equal(Seq(aIsPerson))
  }

  test("returns no predicates if no ids are covered") {
    val selections = Selections(Set(
      Predicate(aId, aIsPerson),
      Predicate(bId, bIsAnimal)
    ))

    selections.predicatesGiven(Set.empty) should equal(Seq.empty)
  }

  test("does not take on a predicate if it is only half covered") {
    val compare = equals(prop("a", "prop1"), prop("b", "prop1"))

    val selections = Selections(Set(
      Predicate(idNames("a", "b"), compare)
    ))

    selections.predicatesGiven(aId) should equal(Seq.empty)
  }

  test("should prune covered sub predicates") {
    val covered = aIsPerson
    val covering = aIsProgrammer
    val selections = Selections(Set(Predicate(aId, PartialPredicate(covered, covering)), Predicate(aId, covering)))

    val expected = Selections(Set(Predicate(aId, covering)))

    selections should equal(expected)
  }

  test("should prune covered sub predicates when adding") {
    val covered = aIsPerson
    val covering = aIsProgrammer
    val selections = Selections(Set(Predicate(aId, PartialPredicate(covered, covering))))
    val newSelections = Selections(Set(Predicate(aId, covering)))

    val fromLeft = selections ++ newSelections
    val fromRight = newSelections ++ selections

    val expected = Selections(Set(Predicate(aId, covering)))

    fromLeft should equal(expected)
    fromRight should equal(expected)
  }

  private def idNames(names: String*): Set[LogicalVariable] = names.toSet.map(varFor)
}
