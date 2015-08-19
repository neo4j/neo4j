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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{InterpolationLiteral, Collection, ASTNode, AstConstructionTestSupport}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class NoUntypedExpressionsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = noUntypedExpressions

  test("reports untyped expressions") {
    val ast: ASTNode = Collection(Seq(InterpolationLiteral("Woof!")_))_

    condition(ast) shouldBe List("Found untyped expression at line 1, column 0 (offset: 0): InterpolationLiteral(Woof!)")
  }

  test("does not report typed expressions") {
    val ast: ASTNode = Collection(Seq(Interpolation(NonEmptyList(Right("Woof!")))_))_

    condition(ast) should be(empty)
  }
}
