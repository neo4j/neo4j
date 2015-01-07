/**
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
package org.neo4j.cypher.internal.compiler.v1_9.commands

import expressions.{Literal, Expression}
import org.neo4j.cypher.internal.compiler.v1_9.mutation._
import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import org.neo4j.cypher.internal.compiler.v1_9.mutation.CreateNode
import org.neo4j.cypher.internal.compiler.v1_9.mutation.CreateRelationship

abstract class StartItem(val identifierName: String) extends TypeSafe with AstNode[StartItem] {
  def mutating : Boolean

  override def addsToRow() = Seq(identifierName)
}

trait ReadOnlyStartItem extends StartItem {

  def mutating = false
  def children:Seq[AstNode[_]] = Nil

  def throwIfSymbolsMissing(symbols: SymbolTable) {}
  def symbolTableDependencies = Set.empty
   
  def rewrite(f: (Expression) => Expression) = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class AllNodes(columnName: String)
  extends StartItem(columnName) with ReadOnlyStartItem

case class AllRelationships(columnName: String)
  extends StartItem(columnName) with ReadOnlyStartItem

//We need to wrap the inner classes to be able to have two different rewrite methods
abstract class UpdatingStartItem(val updateAction:UpdateAction, name:String) extends StartItem(name) {

  override def mutating = true
  override def children = Seq(updateAction)
  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    updateAction.throwIfSymbolsMissing(symbols)
  }
  override def symbolTableDependencies = updateAction.symbolTableDependencies
}

case class CreateNodeStartItem(inner: CreateNode) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateNodeStartItem(inner.rewrite(f))
}

case class CreateRelationshipStartItem(inner: CreateRelationship) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(inner.rewrite(f))
}

case class CreateUniqueStartItem(inner: CreateUniqueAction) extends UpdatingStartItem(inner, "oh noes")  {
  override def rewrite(f: (Expression) => Expression) = CreateUniqueStartItem(inner.rewrite(f))
}

object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}
