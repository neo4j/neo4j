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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.mutation

trait Clause extends AstNode with SemanticCheckable

case class Start(items: Seq[StartItem], token: InputToken) extends Clause {
  def children = items
  def semanticCheck = items.semanticCheck
}

case class Match(patterns: Seq[Pattern], token: InputToken) extends Clause {
  def children = patterns
  def semanticCheck = patterns.semanticCheck(Pattern.SemanticContext.Match)
}

case class Where(expression: Expression, token: InputToken) extends Clause {
  def children = Seq(expression)
  def semanticCheck = expression.semanticCheck(Expression.SemanticContext.Simple)
}


trait UpdateClause extends Clause {
  def toLegacyUpdateActions : Seq[mutation.UpdateAction]
}

case class Create(patterns: Seq[Pattern], token: InputToken) extends UpdateClause {
  def children = patterns
  def semanticCheck = patterns.semanticCheck(Pattern.SemanticContext.Create)

  def toLegacyStartItems : Seq[commands.UpdatingStartItem] = toLegacyUpdateActions.map(_ match {
    case createNode: mutation.CreateNode => commands.CreateNodeStartItem(createNode)
    case createRelationship: mutation.CreateRelationship => commands.CreateRelationshipStartItem(createRelationship)
  })
  def toLegacyUpdateActions = patterns.flatMap(_.toLegacyCreates)
  def toLegacyNamedPaths = patterns.flatMap(_ match {
    case n : NamedPattern => Some(commands.NamedPath(n.identifier.name, n.toLegacyPatterns:_*))
    case _ => None
  })
}


case class Delete(expressions: Seq[Expression], token: InputToken) extends UpdateClause {
  def children = expressions
  def semanticCheck = expressions.semanticCheck(Expression.SemanticContext.Simple)

  def toLegacyUpdateActions = expressions.map(e => mutation.DeleteEntityAction(e.toCommand))
}


case class SetClause(items: Seq[SetItem], token: InputToken) extends UpdateClause {
  def children = items
  def semanticCheck = items.semanticCheck

  def toLegacyUpdateActions = items.map(_.toLegacyUpdateAction)
}


case class Remove(items: Seq[RemoveItem], token: InputToken) extends UpdateClause {
  def children = items
  def semanticCheck = items.semanticCheck

  def toLegacyUpdateActions = items.map(_.toLegacyUpdateAction)
}
