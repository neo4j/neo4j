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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class asPrettyStringTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should interpolate just literal string") {
    pretty"Foo" should equal(asPrettyString("Foo"))
  }

  test("should interpolate with one argument") {
    val p1 = asPrettyString("Bar")
    pretty"Foo$p1" should equal(asPrettyString("FooBar"))
    pretty"${p1}Foo" should equal(asPrettyString("BarFoo"))
  }

  test("should interpolate with two argument") {
    val p1 = asPrettyString("Bar")
    val p2 = asPrettyString("Baz")
    pretty"${p1}Foo$p2" should equal(asPrettyString("BarFooBaz"))
    pretty"Foo$p1$p2" should equal(asPrettyString("FooBarBaz"))
  }

  test("should interpret escape sequences in interpolator") {
    pretty"abc\ndef" should equal(asPrettyString.raw("abc\ndef"))
  }

  test("should remove multiple layers of namespacer renamings") {
    pretty"var" shouldBe asPrettyString("    var@10@20")
  }

  test("should handle CoerceToPredicate") {
    pretty"CoerceToPredicate([1, 2, 3])" shouldBe asPrettyString(CoerceToPredicate(listOfInt(1, 2, 3)))
  }

  test("should handle CoerceToPredicate with container index and a nested plan expression") {
    val nestedPlanExpr = NestedPlanCollectExpression(null, null, "[(a)<-[`anon_2`]-(b) | b.prop4 IN [true]]")(pos)
    pretty"CoerceToPredicate([(a)<-[`anon_2`]-(b) | b.prop4 IN [true]][4])" shouldBe asPrettyString(
      CoerceToPredicate(containerIndex(nestedPlanExpr, literal(4)))
    )
  }
}
