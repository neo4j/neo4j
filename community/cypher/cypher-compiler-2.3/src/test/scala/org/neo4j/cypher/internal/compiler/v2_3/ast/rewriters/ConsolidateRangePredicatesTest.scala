/*
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class ConsolidateRangePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  val n_prop: Expression = Property(ident("n"), PropertyKeyName("prop")_)_

  test("Combines multiple < predicates on the same node and property") {
    // Given
    val actual = Seq(lessThan(n_prop, num(7)), lessThan(n_prop, num(8)))
    val expected = Seq(lessThanOrEqual(n_prop, min(num(7), num(8))), notEquals(n_prop, num(7)), notEquals(n_prop, num(8)))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  test("Combines multiple <= predicates on the same node and property") {
    // Given
    val actual = Seq(lessThanOrEqual(n_prop, num(7)), lessThanOrEqual(n_prop, num(8)))
    val expected = Seq(lessThanOrEqual(n_prop, min(num(7), num(8))))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  test("Combines multiple >= predicates on the same node and property") {
    // Given
    val actual = Seq(greaterThanOrEqual(n_prop, num(7)), greaterThanOrEqual(n_prop, num(8)))
    val expected = Seq(greaterThanOrEqual(n_prop, max(num(7), num(8))))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  test("Combines multiple > predicates on the same node and property") {
    // Given
    val actual = Seq(greaterThan(n_prop, num(7)), greaterThan(n_prop, num(8)))
    val expected = Seq(greaterThanOrEqual(n_prop, max(num(7), num(8))), notEquals(n_prop, num(7)), notEquals(n_prop, num(8)))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  test("should work for a combination of all inequalities") {
    // Given
    val actual = Seq(greaterThan(n_prop, num(1)), greaterThanOrEqual(n_prop, num(2)), lessThan(n_prop, num(10)), lessThanOrEqual(n_prop, num(20)))
    val expected = Seq(greaterThanOrEqual(n_prop, max(num(1), num(2))), notEquals(n_prop, num(1)), lessThanOrEqual(n_prop, min(num(10), num(20))), notEquals(n_prop, num(10)))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  test("should not break the case of single inequalities") {
    // Given
    val lt = Seq(lessThan(n_prop, num(-10)))
    val ltE = Seq(lessThanOrEqual(n_prop, num(-10)))
    val gtE = Seq(greaterThanOrEqual(n_prop, num(-10)))
    val gt = Seq(greaterThan(n_prop, num(-10)))

    // Wham
    Seq(lt, ltE, gtE, gt).foreach { actual =>
      ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(actual)
    }
  }

  test("should not mess up the targets") {
    // Given
    val m_prop: Expression = Property(ident("m"), PropertyKeyName("prop")_)_
    val actual = Seq(greaterThan(n_prop, num(1)), greaterThanOrEqual(n_prop, num(2)), lessThan(m_prop, num(10)), lessThanOrEqual(m_prop, num(20)))
    val expected = Seq(greaterThanOrEqual(n_prop, max(num(1), num(2))), notEquals(n_prop, num(1)), lessThanOrEqual(m_prop, min(num(10), num(20))), notEquals(m_prop, num(10)))

    // Wham
    ConsolidateRangePredicates(actual)(AstRangePredicateToolkit) should equal(expected)
  }

  private def num(v: Int): Expression =
    SignedDecimalIntegerLiteral(v.toString)_

  private def lessThan(l: Expression, r: Expression): Expression =
    LessThan(l, r)_

  private def lessThanOrEqual(l: Expression, r: Expression): Expression =
    LessThanOrEqual(l, r)_

  private def greaterThanOrEqual(l: Expression, r: Expression): Expression =
    GreaterThanOrEqual(l, r)_

  private def greaterThan(l: Expression, r: Expression): Expression =
    GreaterThan(l, r)_

  private def notEquals(l: Expression, r: Expression): Expression = Not(Equals(l, r)_)_

  private def max(l: Expression, r: Expression): Expression = {
    Maximum(Seq(l, r))
  }

  private def min(l: Expression, r: Expression): Expression = {
    Minimum(Seq(l, r))
  }
}
