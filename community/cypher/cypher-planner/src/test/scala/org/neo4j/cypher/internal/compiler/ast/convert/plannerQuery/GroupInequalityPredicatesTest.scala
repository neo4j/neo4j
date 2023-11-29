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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class GroupInequalityPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val n_prop1 = prop("n", "prop1")
  private val m_prop1 = prop("m", "prop1")
  private val m_prop2 = prop("m", "prop2")

  test("Should handle single predicate") {
    groupInequalityPredicates(ListSet(pred(lessThan(n_prop1, literalInt(1))))).toSet should equal(Set(anded(
      n_prop1,
      lessThan(n_prop1, literalInt(1))
    )))
    groupInequalityPredicates(ListSet(pred(lessThanOrEqual(n_prop1, literalInt(1))))).toSet should equal(Set(anded(
      n_prop1,
      lessThanOrEqual(n_prop1, literalInt(1))
    )))
    groupInequalityPredicates(ListSet(pred(greaterThan(n_prop1, literalInt(1))))).toSet should equal(Set(anded(
      n_prop1,
      greaterThan(n_prop1, literalInt(1))
    )))
    groupInequalityPredicates(ListSet(pred(greaterThanOrEqual(n_prop1, literalInt(1))))).toSet should equal(Set(anded(
      n_prop1,
      greaterThanOrEqual(n_prop1, literalInt(1))
    )))
  }

  test("Should group by lhs property") {
    groupInequalityPredicates(ListSet(
      pred(lessThan(n_prop1, literalInt(1))),
      pred(lessThanOrEqual(n_prop1, literalInt(2))),
      pred(lessThan(m_prop1, literalInt(3))),
      pred(greaterThan(m_prop1, literalInt(4))),
      pred(greaterThanOrEqual(m_prop2, literalInt(5)))
    )).toSet should equal(Set(
      anded(n_prop1, lessThan(n_prop1, literalInt(1)), lessThanOrEqual(n_prop1, literalInt(2))),
      anded(m_prop1, lessThan(m_prop1, literalInt(3)), greaterThan(m_prop1, literalInt(4))),
      anded(m_prop2, greaterThanOrEqual(m_prop2, literalInt(5)))
    ))
  }

  test("Should keep other predicates when encountering both inequality and other predicates") {
    groupInequalityPredicates(ListSet(
      pred(lessThan(n_prop1, literalInt(1))),
      pred(equals(n_prop1, literalInt(1)))
    )).toSet should equal(Set(
      anded(n_prop1, lessThan(n_prop1, literalInt(1))),
      pred(equals(n_prop1, literalInt(1)))
    ))
  }

  test("Should keep other predicates when encountering only other predicates") {
    groupInequalityPredicates(ListSet(
      pred(equals(n_prop1, literalInt(1))),
      pred(equals(m_prop2, literalInt(2)))
    )).toSet should equal(Set(
      pred(equals(n_prop1, literalInt(1))),
      pred(equals(m_prop2, literalInt(2)))
    ))
  }

  test("Should not group inequalities on non-property lookups") {
    groupInequalityPredicates(ListSet(
      pred(lessThan(varFor("x"), literalInt(1))),
      pred(greaterThanOrEqual(varFor("x"), literalInt(1)))
    )).toSet should equal(Set(
      pred(lessThan(varFor("x"), literalInt(1))),
      pred(greaterThanOrEqual(varFor("x"), literalInt(1)))
    ))
  }

  test("Should group inside an Ands") {
    groupInequalityPredicates(ListSet(
      pred(equals(n_prop1, literalInt(1))),
      pred(ors(ands(
        lessThan(m_prop1, literalInt(3)),
        greaterThan(m_prop1, literalInt(4)),
        equals(m_prop2, literalInt(2))
      ))),
      pred(greaterThanOrEqual(m_prop2, literalInt(5)))
    )).toSet should equal(Set(
      pred(equals(n_prop1, literalInt(1))),
      pred(ors(ands(
        anded(m_prop1, lessThan(m_prop1, literalInt(3)), greaterThan(m_prop1, literalInt(4))).expr,
        equals(m_prop2, literalInt(2))
      ))),
      anded(m_prop2, greaterThanOrEqual(m_prop2, literalInt(5)))
    ))
  }

  test("Should group inside an Ands and squash if only one expression remains") {
    groupInequalityPredicates(ListSet(
      pred(equals(n_prop1, literalInt(1))),
      pred(ors(ands(
        lessThan(m_prop1, literalInt(3)),
        greaterThan(m_prop1, literalInt(4))
      ))),
      pred(greaterThanOrEqual(m_prop2, literalInt(5)))
    )).toSet should equal(Set(
      pred(equals(n_prop1, literalInt(1))),
      pred(ors(
        anded(m_prop1, lessThan(m_prop1, literalInt(3)), greaterThan(m_prop1, literalInt(4))).expr
      )),
      anded(m_prop2, greaterThanOrEqual(m_prop2, literalInt(5)))
    ))
  }

  private def pred(expr: Expression) =
    Predicate(expr.dependencies.map { ident => ident }, expr)

  private def anded(property: Property, first: InequalityExpression, others: InequalityExpression*) = {
    val variable = property.map.asInstanceOf[Variable]
    val inequalities = NonEmptyList(first, others: _*)
    val deps = others.foldLeft(first.dependencies) { (acc, elem) => acc ++ elem.dependencies }
    Predicate(deps, AndedPropertyInequalities(variable, property, inequalities))
  }
}
