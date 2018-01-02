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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class GroupInequalityPredicatesForLegacyTest extends CypherFunSuite {

  val n_prop1: Property = Property(Identifier("n"), PropertyKey("prop1"))
  val m_prop1: Property = Property(Identifier("m"), PropertyKey("prop1"))
  val m_prop2: Property = Property(Identifier("m"), PropertyKey("prop2"))

  test("Should handle single predicate") {
    groupInequalityPredicatesForLegacy(NonEmptyList(lessThan(n_prop1, 1))).toSet should equal(NonEmptyList(anded(n_prop1, lessThan(n_prop1, 1))).toSet)
    groupInequalityPredicatesForLegacy(NonEmptyList(lessThanOrEqual(n_prop1, 1))).toSet should equal(NonEmptyList(anded(n_prop1, lessThanOrEqual(n_prop1, 1))).toSet)
    groupInequalityPredicatesForLegacy(NonEmptyList(greaterThan(n_prop1, 1))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThan(n_prop1, 1))).toSet)
    groupInequalityPredicatesForLegacy(NonEmptyList(greaterThanOrEqual(n_prop1, 1))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThanOrEqual(n_prop1, 1))).toSet)
  }

  test("Should group by lhs property") {
    groupInequalityPredicatesForLegacy(NonEmptyList(
      lessThan(n_prop1, 1),
      lessThanOrEqual(n_prop1, 2),
      lessThan(m_prop1, 3),
      greaterThan(m_prop1, 4),
      greaterThanOrEqual(m_prop2, 5)
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, 1), lessThanOrEqual(n_prop1, 2)),
      anded(m_prop1, lessThan(m_prop1, 3), greaterThan(m_prop1, 4)),
      anded(m_prop2, greaterThanOrEqual(m_prop2, 5))
    ).toSet)
  }

  test("Should keep other predicates when encountering both inequality and other predicates") {
    groupInequalityPredicatesForLegacy(NonEmptyList(
      lessThan(n_prop1, 1),
      equals(n_prop1, 1)
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, 1)),
      equals(n_prop1, 1)
    ).toSet)
  }

  test("Should keep other predicates when encountering only other predicates") {
    groupInequalityPredicatesForLegacy(NonEmptyList(
      equals(n_prop1, 1),
      equals(m_prop2, 2)
    )).toSet should equal(NonEmptyList(
      equals(n_prop1, 1),
      equals(m_prop2, 2)
    ).toSet)
  }

  test("Should not group inequalities on non-property lookups") {
    groupInequalityPredicatesForLegacy(NonEmptyList(
      lessThan(Identifier("x"), 1),
      greaterThanOrEqual(Identifier("x"), 1)
    )).toSet should equal(NonEmptyList(
      lessThan(Identifier("x"), 1),
      greaterThanOrEqual(Identifier("x"), 1)
    ).toSet)
  }

  private def equals(lhs: Expression, v: Int) =
    Equals(lhs, Literal(v))

  private def lessThan(lhs: Expression, v: Int) =
    LessThan(lhs, Literal(v))

  private def lessThanOrEqual(lhs: Expression, v: Int) =
    LessThanOrEqual(lhs, Literal(v))

  private def greaterThan(lhs: Expression, v: Int) =
    GreaterThan(lhs, Literal(v))

  private def greaterThanOrEqual(lhs: Expression, v: Int) =
    GreaterThanOrEqual(lhs, Literal(v))

  private def anded(property: Property, first: ComparablePredicate, others: ComparablePredicate*) = {
    val identifier = property.mapExpr.asInstanceOf[Identifier]
    val inequalities = NonEmptyList(first, others: _*)
    AndedPropertyComparablePredicates(identifier, property, inequalities)
  }
}
