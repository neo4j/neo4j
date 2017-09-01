/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast.Variable
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.TestName
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, ast}
import org.parboiled.scala._

trait ParserAstTest[AST] extends ParserTest[AST, AST] with TestName {
  final override def convert(ast: AST): AST = ast

  final def yields(expr: (InputPosition) => AST)(implicit parser: Rule1[AST]): Unit = parsing(testName) shouldGive expr

  final def failsToParse(implicit parser: Rule1[AST]): Unit = assertFails(testName)

  private type Expression = (InputPosition) => ast.Expression

  final def id(id: String): (InputPosition) => Variable = ast.Variable(id)(_)

  final def lt(lhs: Expression, rhs: Expression): Expression = { pos => ast.LessThan(lhs(pos), rhs(pos))(pos) }

  final def lte(lhs: Expression, rhs: Expression): Expression = { pos => ast.LessThanOrEqual(lhs(pos), rhs(pos))(pos) }

  final def gt(lhs: Expression, rhs: Expression): Expression = { pos => ast.GreaterThan(lhs(pos), rhs(pos))(pos) }

  final def gte(lhs: Expression, rhs: Expression): Expression = { pos => ast.GreaterThanOrEqual(lhs(pos), rhs(pos))(pos) }

  final def eq(lhs: Expression, rhs: Expression): Expression = { pos => ast.Equals(lhs(pos), rhs(pos))(pos) }

  final def ne(lhs: Expression, rhs: Expression): Expression = { pos => ast.NotEquals(lhs(pos), rhs(pos))(pos) }

  final def and(lhs: Expression, rhs: Expression): Expression = { pos => ast.And(lhs(pos), rhs(pos))(pos) }

  final def ands(parts: Expression*): Expression = { pos => ast.Ands(parts.map(_(pos)).toSet)(pos) }
}
