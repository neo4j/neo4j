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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.v4_0.util.NonEmptyList
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.v4_0.expressions._

class GroupInequalityPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val n_prop1 = prop("n", "prop1")
  private val m_prop1 = prop("m", "prop1")
  private val m_prop2 = prop("m", "prop2")

  test("Should handle single predicate") {
    groupInequalityPredicates(NonEmptyList(pred(lessThan(n_prop1, literalInt(1))))).toSet should equal(NonEmptyList(anded(n_prop1, lessThan(n_prop1, literalInt(1)))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(lessThanOrEqual(n_prop1, literalInt(1))))).toSet should equal(NonEmptyList(anded(n_prop1, lessThanOrEqual(n_prop1, literalInt(1)))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(greaterThan(n_prop1, literalInt(1))))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThan(n_prop1, literalInt(1)))).toSet)
    groupInequalityPredicates(NonEmptyList(pred(greaterThanOrEqual(n_prop1, literalInt(1))))).toSet should equal(NonEmptyList(anded(n_prop1, greaterThanOrEqual(n_prop1, literalInt(1)))).toSet)
  }

  test("Should group by lhs property") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(n_prop1, literalInt(1))),
      pred(lessThanOrEqual(n_prop1, literalInt(2))),
      pred(lessThan(m_prop1, literalInt(3))),
      pred(greaterThan(m_prop1, literalInt(4))),
      pred(greaterThanOrEqual(m_prop2, literalInt(5)))
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, literalInt(1)), lessThanOrEqual(n_prop1, literalInt(2))),
      anded(m_prop1, lessThan(m_prop1, literalInt(3)), greaterThan(m_prop1, literalInt(4))),
      anded(m_prop2, greaterThanOrEqual(m_prop2, literalInt(5)))
    ).toSet)
  }

  test("Should keep other predicates when encountering both inequality and other predicates") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(n_prop1, literalInt(1))),
      pred(equals(n_prop1, literalInt(1)))
    )).toSet should equal(NonEmptyList(
      anded(n_prop1, lessThan(n_prop1, literalInt(1))),
      pred(equals(n_prop1, literalInt(1)))
    ).toSet)
  }

  test("Should keep other predicates when encountering only other predicates") {
    groupInequalityPredicates(NonEmptyList(
      pred(equals(n_prop1, literalInt(1))),
      pred(equals(m_prop2, literalInt(2)))
    )).toSet should equal(NonEmptyList(
      pred(equals(n_prop1, literalInt(1))),
      pred(equals(m_prop2, literalInt(2)))
    ).toSet)
  }

  test("Should not group inequalities on non-property lookups") {
    groupInequalityPredicates(NonEmptyList(
      pred(lessThan(varFor("x"), literalInt(1))),
      pred(greaterThanOrEqual(varFor("x"), literalInt(1)))
    )).toSet should equal(NonEmptyList(
      pred(lessThan(varFor("x"), literalInt(1))),
      pred(greaterThanOrEqual(varFor("x"), literalInt(1)))
    ).toSet)
  }

  private def pred(expr: Expression) =
    Predicate(expr.dependencies.map { ident => ident.name }, expr)

  private def anded(property: Property, first: InequalityExpression, others: InequalityExpression*) = {
    val variable = property.map.asInstanceOf[Variable]
    val inequalities = NonEmptyList(first, others: _*)
    val deps = others.foldLeft(first.dependencies) { (acc, elem) => acc ++ elem.dependencies }.map { ident => ident.name }
    Predicate(deps, AndedPropertyInequalities(variable, property, inequalities))
  }
}
