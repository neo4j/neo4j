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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.TestName
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, ast}
import org.parboiled.scala._

trait ParserAstTest[AST] extends ParserTest[AST, AST] with TestName {
  final override def convert(ast: AST): AST = ast

  final def yields(expr: (InputPosition) => AST)(implicit parser: Rule1[AST]) = parsing(testName) shouldGive expr

  private type Expression = (InputPosition) => ast.Expression

  final def id(id: String) = ast.Identifier(id)(_)

  final def lt(lhs: Expression, rhs: Expression): Expression = { pos => ast.LessThan(lhs(pos), rhs(pos))(pos) }

  final def lte(lhs: Expression, rhs: Expression): Expression = { pos => ast.LessThanOrEqual(lhs(pos), rhs(pos))(pos) }

  final def gt(lhs: Expression, rhs: Expression): Expression = { pos => ast.GreaterThan(lhs(pos), rhs(pos))(pos) }

  final def gte(lhs: Expression, rhs: Expression): Expression = { pos => ast.GreaterThanOrEqual(lhs(pos), rhs(pos))(pos) }

  final def eq(lhs: Expression, rhs: Expression): Expression = { pos => ast.Equals(lhs(pos), rhs(pos))(pos) }

  final def ne(lhs: Expression, rhs: Expression): Expression = { pos => ast.NotEquals(lhs(pos), rhs(pos))(pos) }

  final def and(lhs: Expression, rhs: Expression): Expression = { pos => ast.And(lhs(pos), rhs(pos))(pos) }

  final def ands(parts: Expression*): Expression = { pos => ast.Ands(parts.map(_(pos)).toSet)(pos) }
}
