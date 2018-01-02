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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_3.planner.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class GroupInequalityPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  val n_prop1: Property = Property(ident("n"), PropertyKeyName("prop1")_)_
  val m_prop1: Property = Property(ident("m"), PropertyKeyName("prop1")_)_
  val m_prop2: Property = Property(ident("m"), PropertyKeyName("prop2")_)_

  test("Should handle single predicate") {
    groupInequalityPredicates(NonEmptyList(pred(lessThan(n_prop1, 1)))).toSet should equal(NonEmptyList(anded(n_prop1, lessThan(n_prop1, 1))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(lessThanOrEqual(n_prop1, 1)))).toSet should equal(NonEmptyList(anded(n_prop1, lessThanOrEqual(n_prop1, 1))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(greaterThan(n_prop1, 1)))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThan(n_prop1, 1))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(greaterThanOrEqual(n_prop1, 1)))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThanOrEqual(n_prop1, 1))).toSet)
  }

  test("Should group by lhs property") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(n_prop1, 1)),
      pred(lessThanOrEqual(n_prop1, 2)),
      pred(lessThan(m_prop1, 3)),
      pred(greaterThan(m_prop1, 4)),
      pred(greaterThanOrEqual(m_prop2, 5))
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, 1), lessThanOrEqual(n_prop1, 2)),
      anded(m_prop1, lessThan(m_prop1, 3), greaterThan(m_prop1, 4)),
      anded(m_prop2, greaterThanOrEqual(m_prop2, 5))
    ).toSet)
  }

  test("Should keep other predicates when encountering both inequality and other predicates") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(n_prop1, 1)),
      pred(equals(n_prop1, 1))
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, 1)),
      pred(equals(n_prop1, 1))
    ).toSet)
  }

  test("Should keep other predicates when encountering only other predicates") {
    groupInequalityPredicates(NonEmptyList(
      pred(equals(n_prop1, 1)),
      pred(equals(m_prop2, 2))
    )).toSet should equal(NonEmptyList(
      pred(equals(n_prop1, 1)),
      pred(equals(m_prop2, 2))
    ).toSet)
  }

  test("Should not group inequalities on non-property lookups") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(ident("x"), 1)),
      pred(greaterThanOrEqual(ident("x"), 1))
    )).toSet should equal(NonEmptyList(
      pred(lessThan(ident("x"), 1)),
      pred(greaterThanOrEqual(ident("x"), 1))
    ).toSet)
  }

  private def equals(lhs: Expression, v: Int) =
    Equals(lhs, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def lessThan(lhs: Expression, v: Int) =
    LessThan(lhs, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def lessThanOrEqual(lhs: Expression, v: Int) =
    LessThanOrEqual(lhs, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def greaterThan(lhs: Expression, v: Int) =
    GreaterThan(lhs, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def greaterThanOrEqual(lhs: Expression, v: Int) =
    GreaterThanOrEqual(lhs, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def pred(expr: Expression) =
    Predicate(expr.dependencies.map { ident => IdName(ident.name) }, expr)

  private def anded(property: Property, first: InequalityExpression, others: InequalityExpression*) = {
    val identifier = property.map.asInstanceOf[Identifier]
    val inequalities = NonEmptyList(first, others: _*)
    val deps = others.foldLeft(first.dependencies) { (acc, elem) => acc ++ elem.dependencies }.map { ident => IdName(ident.name) }
    Predicate(deps, AndedPropertyInequalities(identifier, property, inequalities))
  }
}
