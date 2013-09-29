/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0.ast

import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.symbols.AnyType
import org.neo4j.cypher.SyntaxException

case class Where(expression: Expression, token: InputToken) extends AstNode with SemanticCheckable {
  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
      expression.constrainType(AnyType()) // TODO: should constrain to boolean, when coercion is possible

  def toLegacyPredicate = {
    expression.toCommand match {
      case p: commands.Predicate => p
      case _                     => throw new SyntaxException(s"WHERE clause expression must return a boolean (${expression.token.startPosition})")
    }
  }
}
