/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_0.mutation._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.data.SimpleVal
import org.neo4j.cypher.internal.helpers.Materialized
import org.neo4j.cypher.internal.compiler.v2_0.data.SimpleVal._
import org.neo4j.cypher.internal.compiler.v2_0.mutation.MergeNodeAction
import org.neo4j.cypher.internal.compiler.v2_0.mutation.CreateUniqueAction
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.mutation.CreateNode
import org.neo4j.cypher.internal.compiler.v2_0.data.SeqVal
import org.neo4j.cypher.internal.compiler.v2_0.mutation.CreateRelationship
import java.net.URL

trait NodeStartItemIdentifiers extends StartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTNode)
}

trait RelationshipStartItemIdentifiers extends StartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTRelationship)
}

abstract class StartItem(val identifierName: String, val args: Map[String, String])
  extends TypeSafe with AstNode[StartItem] {
  def mutating: Boolean
  def producerType: String = getClass.getSimpleName
  def identifiers: Seq[(String, CypherType)]

  def description: Seq[(String, SimpleVal)] = {
    val argValues = Materialized.mapValues(args, fromStr).toSeq
    val otherValues = Seq(
      "producer" -> SimpleVal.fromStr(producerType),
      "identifiers" -> SeqVal(identifiers.toMap.keys.map(SimpleVal.fromStr).toSeq)
    )
    argValues ++ otherValues
  }
}

trait ReadOnlyStartItem {
  def mutating = false

  // AstNode implementations
  def children: Seq[AstNode[_]] = Nil
  def symbolTableDependencies:Set[String] = Set.empty
  def rewrite(f: (Expression) => Expression):this.type = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName, Map.empty) with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "key" -> key.toString(), "expr" -> expression.toString()))
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "query" -> query.toString()))
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "key" -> key.toString(), "expr" -> expression.toString()))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Map("idxName" -> idxName, "query" -> query.toString()))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

trait Hint

sealed abstract class SchemaIndexKind

case object AnyIndex extends SchemaIndexKind
case object UniqueIndex extends SchemaIndexKind

case class SchemaIndex(identifier: String, label: String, property: String, kind: SchemaIndexKind, query: Option[Expression])
  extends StartItem(identifier, Map("label" -> label, "property" -> property) ++ query.map("query" -> _.toString()))
  with ReadOnlyStartItem with Hint with NodeStartItemIdentifiers

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName, Map("name" -> expression.toString()))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByIdOrEmpty(varName: String, expression: Expression)
  extends StartItem(varName, Map("name" -> expression.toString()))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByLabel(varName: String, label: String)
  extends StartItem(varName, Map("label" -> label.toString))
  with ReadOnlyStartItem with Hint with NodeStartItemIdentifiers

case class AllNodes(columnName: String) extends StartItem(columnName, Map.empty)
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class AllRelationships(columnName: String) extends StartItem(columnName, Map.empty)
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class LoadCSV(withHeaders: Boolean, fileUrl: URL, identifier: String) extends StartItem(identifier, Map.empty)
  with ReadOnlyStartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> (if (withHeaders) CTMap else CTCollection(CTAny)))
}
case class Unwind(expression: Expression, identifier: String) extends StartItem(identifier, Map.empty)
  with ReadOnlyStartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTAny)
}

//We need to wrap the inner classes to be able to have two different rewrite methods
abstract class UpdatingStartItem(val updateAction: UpdateAction, name: String) extends StartItem(name, Map.empty) {

  override def mutating = true
  override def children = Seq(updateAction)
  override def symbolTableDependencies = updateAction.symbolTableDependencies

  def identifiers: Seq[(String, CypherType)] = updateAction.identifiers
}

case class CreateNodeStartItem(inner: CreateNode) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateNodeStartItem(inner.rewrite(f))
}

case class CreateRelationshipStartItem(inner: CreateRelationship) extends UpdatingStartItem(inner, inner.key) {
  override def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(inner.rewrite(f))
}

case class CreateUniqueStartItem(inner: CreateUniqueAction) extends UpdatingStartItem(inner, "oh noes") {
  override def rewrite(f: (Expression) => Expression) = CreateUniqueStartItem(inner.rewrite(f))
}

case class MergeNodeStartItem(inner: MergeNodeAction) extends UpdatingStartItem(inner, inner.identifier) {
  override def rewrite(f: (Expression) => Expression) = MergeNodeStartItem(inner.rewrite(f))
}

/** NodeById that throws exception if no node is found */
object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}
