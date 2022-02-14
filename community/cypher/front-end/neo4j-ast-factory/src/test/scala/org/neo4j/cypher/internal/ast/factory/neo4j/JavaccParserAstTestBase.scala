/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName

trait JavaccParserAstTestBase[AST <: ASTNode] extends JavaccParserTestBase[AST, AST] with TestName with AstConstructionTestSupport with VerifyAstPositionTestSupport {

  final override def convert(astNode: AST): AST = astNode

  final def yields(expr: InputPosition => AST)(implicit parser: JavaccRule[AST]): Unit = parsing(testName) shouldGive expr

  final def gives(ast: AST)(implicit parser: JavaccRule[AST]): Unit = parsing(testName) shouldGive ast

  final def givesIncludingPositions(expected: AST)(implicit parser: JavaccRule[AST]): Unit = {
    parsing(testName) shouldVerify { actual =>
      actual shouldBe expected
      verifyPositions(actual, expected)
    }
  }

  final def failsToParse(implicit parser: JavaccRule[AST]): Unit = assertFails(testName)

  final def id(id: String): Variable = varFor(id)

  final def lt(lhs: Expression, rhs: Expression): Expression = lessThan(lhs, rhs)

  final def lte(lhs: Expression, rhs: Expression): Expression = lessThanOrEqual(lhs, rhs)

  final def gt(lhs: Expression, rhs: Expression): Expression = greaterThan(lhs, rhs)

  final def gte(lhs: Expression, rhs: Expression): Expression = greaterThanOrEqual(lhs, rhs)

  final def eq(lhs: Expression, rhs: Expression): Expression = equals(lhs, rhs)

  final def ne(lhs: Expression, rhs: Expression): Expression = notEquals(lhs, rhs)
}
