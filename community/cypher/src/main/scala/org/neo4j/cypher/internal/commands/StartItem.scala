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

import org.neo4j.cypher.internal.symbols.{Identifier, AnyType}


abstract sealed class StartItem(val identifierName:String) {
  def mutating = false
}

abstract class RelationshipStartItem(id:String) extends StartItem(id)
abstract class NodeStartItem(id:String) extends StartItem(id)

case class RelationshipById(varName:String, expression: Expression) extends RelationshipStartItem(varName)
case class RelationshipByIndex(varName:String, idxName: String, key:Expression, expression: Expression) extends RelationshipStartItem(varName)
case class RelationshipByIndexQuery(varName:String, idxName: String, query: Expression) extends RelationshipStartItem(varName)

case class NodeByIndex(varName:String, idxName: String, key:Expression, expression: Expression) extends NodeStartItem(varName)
case class NodeByIndexQuery(varName:String, idxName: String, query: Expression) extends NodeStartItem(varName)
case class NodeById(varName:String, expression:Expression) extends NodeStartItem(varName)

case class AllNodes(columnName:String) extends NodeStartItem(columnName)
case class AllRelationships(columnName:String) extends RelationshipStartItem(columnName)

case class CreateNodeStartItem(varName: String, properties: Map[String, Expression])
  extends NodeStartItem(varName)
  with Mutator
  with UpdateCommand {
  def dependencies: Seq[Identifier] = properties.values.flatMap(_.dependencies(AnyType())).toSeq

  def filter(f: (Expression) => Boolean) = properties.values.filter(f).toSeq

  def rewrite(f: (Expression) => Expression) = CreateNodeStartItem(varName, properties.map(mapRewrite(f)))
}

case class CreateRelationshipStartItem(varName: String, from: Expression, to: Expression, typ: String, properties: Map[String, Expression])
  extends NodeStartItem(varName)
  with Mutator
  with UpdateCommand {
  def dependencies: Seq[Identifier] = properties.values.flatMap(_.dependencies(AnyType())).toSeq ++
    from.dependencies(AnyType()) ++
    to.dependencies(AnyType())

  def filter(f: (Expression) => Boolean) =  {
    val fromSeq = if (f(from)) Seq(from) else Seq()
    val toSeq = if (f(to)) Seq(to) else Seq()

    val values: Iterable[Expression] = properties.values
    fromSeq ++ toSeq ++ values.filter(f).toSet
  }

  def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(varName, f(from), f(to), typ, properties.map(mapRewrite(f)))
}

trait Mutator extends StartItem {
  override def mutating = true

  def mapRewrite(f: (Expression) => Expression)(kv:(String, Expression)):(String,Expression) = kv match { case (k,v) => (k, f(v)) }
}

object NodeById {
  def apply(varName:String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName:String, id: Long*) = new RelationshipById(varName, Literal(id))
}

