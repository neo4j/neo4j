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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.ast.functions.Exists
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop IS NOT NULL rewritten to: exists(a.prop)") {
    val input: Expression = IsNotNull(Property(variable("a"), PropertyKeyName("prop")_)_)_
    val output: Expression = Exists.asInvocation(Property(variable("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("exists(a.prop) is not rewritten") {
    val input: Expression = Exists.asInvocation(Property(variable("a"), PropertyKeyName("prop")_)_)(pos)
    val output: Expression = Exists.asInvocation(Property(variable("a"), PropertyKeyName("prop")_)_)(pos)

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x < y rewritten to: x >= y") {
    val input: Expression = Not(LessThan(variable("x"), variable("y"))_)_
    val output: Expression = GreaterThanOrEqual(variable("x"), variable("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x <= y rewritten to: x > y") {
    val input: Expression = Not(LessThanOrEqual(variable("x"), variable("y"))_)_
    val output: Expression = GreaterThan(variable("x"), variable("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x > y rewritten to: x <= y") {
    val input: Expression = Not(GreaterThan(variable("x"), variable("y"))_)_
    val output: Expression = LessThanOrEqual(variable("x"), variable("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }

  test("NOT x >= y rewritten to: x < y") {
    val input: Expression = Not(GreaterThanOrEqual(variable("x"), variable("y"))_)_
    val output: Expression = LessThan(variable("x"), variable("y"))_

    normalizeSargablePredicates(input) should equal(output)
  }
}
