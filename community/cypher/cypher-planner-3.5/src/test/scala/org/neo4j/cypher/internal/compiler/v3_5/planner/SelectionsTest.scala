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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.ir.v3_5.{Predicate, Selections}
import org.neo4j.cypher.internal.v3_5.expressions._

class SelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {

  val aIsPerson: HasLabels = identHasLabel("a", "Person")
  val aIsProgrammer: HasLabels = identHasLabel("a", "Programmer")
  val bIsAnimal: HasLabels = identHasLabel("b", "Animal")
  val compareTwoNodes: Equals = compareBothSides("a", "b")

  test("can flat predicates to a sequence") {
    val selections = Selections(Set(Predicate(idNames("a"), aIsPerson)))

    selections.flatPredicates should equal(Seq(aIsPerson))
  }

  test("can flat empty predicates to an empty sequence") {
    Selections().flatPredicates should equal(Seq())
  }

  test("should be able to sense that predicates are not covered") {
    val selections = Selections(Set(
      Predicate(idNames("a"), aIsPerson),
      Predicate(idNames("b"), bIsAnimal)
    ))

    selections.coveredBy(Seq(aIsPerson)) should be(right = false)
  }

  test("should be able to tell when all predicates are covered") {
    val selections = Selections(Set(
      Predicate(idNames("a"), aIsPerson)
    ))

    selections.coveredBy(Seq(aIsPerson)) should be(right = true)
  }

  test("can extract HasLabels Predicates") {
    val selections = Selections(Set(
      Predicate(idNames("a"), aIsPerson),
      Predicate(idNames("a"), aIsPerson),
      Predicate(idNames("b"), bIsAnimal),
      Predicate(idNames("c"), Equals(Variable("c") _, SignedDecimalIntegerLiteral("42") _) _)
    ))

    selections.labelPredicates should equal(Map(
      "a" -> Set(aIsPerson),
      "b" -> Set(bIsAnimal)
    ))
  }

  test("can find predicates given covered ids") {
    val a = idNames("a")
    val b = idNames("b")

    val selections = Selections(Set(
      Predicate(a, aIsPerson),
      Predicate(b, bIsAnimal)
    ))

    selections.predicatesGiven(a) should equal(Seq(aIsPerson))
  }

  test("returns no predicates if no ids are covered") {
    val a = idNames("a")
    val b = idNames("b")

    val selections = Selections(Set(
      Predicate(a, aIsPerson),
      Predicate(b, bIsAnimal)
    ))

    selections.predicatesGiven(Set.empty) should equal(Seq.empty)
  }

  test("does not take on a predicate if it is only half covered") {
    val aAndB = idNames("a", "b")
    val a = Set(aAndB.head)

    val selections = Selections(Set(
      Predicate(aAndB, compareTwoNodes)
    ))

    selections.predicatesGiven(a) should equal(Seq.empty)
  }

  test("prunes away sub predicates") {
    val covering = And(aIsPerson, aIsProgrammer)(pos)
    val covered = aIsProgrammer
    val selections = Selections(Set(Predicate(idNames("a"), PartialPredicate(covered, covering))))

    val result = selections ++ Selections(Set(Predicate(idNames("a"), covering)))

    result should equal(Selections(Set(Predicate(idNames("a"), covering))))
  }


  private def idNames(names: String*) = names.toSet

  private def compareBothSides(left: String, right: String) =
    Equals(prop(left, "prop1"), prop(right, "prop1"))(pos)
}
