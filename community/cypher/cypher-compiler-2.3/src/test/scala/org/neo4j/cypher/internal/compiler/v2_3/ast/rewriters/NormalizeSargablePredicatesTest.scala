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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.{Exists, Has}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop IS NOT NULL rewritten to: exists(a.prop)") {
    val input: Expression = IsNotNull(Property(ident("a"), PropertyKeyName("prop")_)_)_
    val output: Expression = Exists.asInvocation(Property(ident("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("has(a.prop) is rewritten to: exists(a.prop)") {
    val input: Expression = Has.asInvocation(Property(ident("a"), PropertyKeyName("prop")_)_)(pos)
    val output: Expression = Exists.asInvocation(Property(ident("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("exists(a.prop) is not rewritten") {
    val input: Expression = Exists.asInvocation(Property(ident("a"), PropertyKeyName("prop")_)_)(pos)
    val output: Expression = Exists.asInvocation(Property(ident("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x < y rewritten to: x >= y") {
    val input: Expression = Not(LessThan(ident("x"), ident("y"))_)_
    val output: Expression = GreaterThanOrEqual(ident("x"), ident("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input: Expression = Not(LessThanOrEqual(ident("x"), ident("y"))_)_
    val output: Expression = GreaterThan(ident("x"), ident("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input: Expression = Not(GreaterThan(ident("x"), ident("y"))_)_
    val output: Expression = LessThanOrEqual(ident("x"), ident("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input: Expression = Not(GreaterThanOrEqual(ident("x"), ident("y"))_)_
    val output: Expression = LessThan(ident("x"), ident("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }
}
