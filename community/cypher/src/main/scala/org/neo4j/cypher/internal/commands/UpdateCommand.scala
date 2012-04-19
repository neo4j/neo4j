/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.internal.symbols.{AnyIterableType, AnyType, MapType, Identifier}


trait UpdateCommand {
  def rewrite(f: Expression => Expression):UpdateCommand
  def filter(f: Expression => Boolean): Seq[Expression]
  def dependencies : Seq[Identifier]
}

case class DeleteEntityCommand(expression:Expression) extends UpdateCommand {
  def rewrite(f: (Expression) => Expression) = DeleteEntityCommand(expression.rewrite(f))

  def dependencies = expression.dependencies(MapType())

  def filter(f: (Expression) => Boolean) = expression.filter(f)
}

case class SetProperty(property: Expression, value: Expression) extends UpdateCommand {
  def dependencies = property.dependencies(AnyType()) ++ value.dependencies(AnyType())

  def filter(f: (Expression) => Boolean) = property.filter(f) ++ value.filter(f)

  def rewrite(f: (Expression) => Expression) = SetProperty(property, value.rewrite(f))
}

case class Foreach(iterableExpression: Expression, symbol: String, commands: Seq[UpdateCommand]) extends UpdateCommand {
  def dependencies = iterableExpression.dependencies(AnyIterableType()) ++ commands.flatMap(_.dependencies).filterNot(_.name == symbol)

  def filter(f: (Expression) => Boolean) = Some(iterableExpression).filter(f).toSeq ++ commands.flatMap(_.filter(f))

  def rewrite(f: (Expression) => Expression) = Foreach(f(iterableExpression), symbol, commands.map(_.rewrite(f)))
}