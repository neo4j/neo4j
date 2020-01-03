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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.{Predicate, Selections}
import org.neo4j.cypher.internal.v4_0.expressions.PartialPredicate

class SelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val aIsPerson = hasLabels("a", "Person")
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
      "a" -> Set(aIsPerson),
      "b" -> Set(bIsAnimal)
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

  test("prunes away sub predicates") {
    val covered = hasLabels("a", "Programmer")
    val covering = and(aIsPerson, covered)
    val selections = Selections(Set(Predicate(aId, PartialPredicate(covered, covering))))

    val result = selections ++ Selections(Set(Predicate(aId, covering)))

    result should equal(Selections(Set(Predicate(aId, covering))))
  }

  private def idNames(names: String*) = names.toSet
}
