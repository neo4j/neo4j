/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.rewriters

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.rewriting.rewriters.normalizeSargablePredicates
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Exists

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop IS NOT NULL rewritten to: exists(a.prop)") {
    val input: Expression = IsNotNull(Property(varFor("a"), PropertyKeyName("prop")_)_)_
    val output: Expression = Exists.asInvocation(Property(varFor("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("exists(a.prop) is not rewritten") {
    val input: Expression = Exists.asInvocation(Property(varFor("a"), PropertyKeyName("prop")_)_)(pos)
    val output: Expression = Exists.asInvocation(Property(varFor("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x < y rewritten to: x >= y") {
    val input: Expression = Not(LessThan(varFor("x"), varFor("y"))_)_
    val output: Expression = GreaterThanOrEqual(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input: Expression = Not(LessThanOrEqual(varFor("x"), varFor("y"))_)_
    val output: Expression = GreaterThan(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input: Expression = Not(GreaterThan(varFor("x"), varFor("y"))_)_
    val output: Expression = LessThanOrEqual(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input: Expression = Not(GreaterThanOrEqual(varFor("x"), varFor("y"))_)_
    val output: Expression = LessThan(varFor("x"), varFor("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }
}
