/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.antlr.v4.runtime.ParserRuleContext
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName

trait ParserSyntaxTreeBase[CST <: ParserRuleContext, AST <: ASTNode] extends ParserTestBase[CST, AST, AST] with TestName
    with AstConstructionTestSupport with VerifyAstPositionTestSupport {

  final override def convert(astNode: AST): AST = astNode

  final def yields(expr: InputPosition => AST)(implicit javaccRule: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit =
    parsing(testName) shouldGive expr

  final def gives(ast: AST)(implicit parser: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit =
    parsing(testName) shouldGive ast

  final def givesIncludingPositions(expected: AST, query: String = testName)(
    implicit javaccRule: JavaccRule[AST],
    antlrRule: AntlrRule[CST]
  ): Unit = {
    parsing(query) shouldVerify { actual =>
      actual shouldBe expected
      verifyPositions(actual, expected)
    }
  }

  final def failsToParse(query: String)(implicit parser: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit =
    assertFails(query)

  final def failsToParse(implicit parser: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit = assertFails(testName)

  final def failsToParseOnlyJavaCC(query: String)(implicit parser: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit =
    assertFailsOnlyJavaCC(query)

  final def failsToParseOnlyJavaCC(implicit parser: JavaccRule[AST], antlrRule: AntlrRule[CST]): Unit =
    assertFailsOnlyJavaCC(testName)

  final def id(id: String): Variable = varFor(id)

  final def lt(lhs: Expression, rhs: Expression): Expression = lessThan(lhs, rhs)

  final def lte(lhs: Expression, rhs: Expression): Expression = lessThanOrEqual(lhs, rhs)

  final def gt(lhs: Expression, rhs: Expression): Expression = greaterThan(lhs, rhs)

  final def gte(lhs: Expression, rhs: Expression): Expression = greaterThanOrEqual(lhs, rhs)

  final def eq(lhs: Expression, rhs: Expression): Expression = equals(lhs, rhs)

  final def ne(lhs: Expression, rhs: Expression): Expression = notEquals(lhs, rhs)
}
