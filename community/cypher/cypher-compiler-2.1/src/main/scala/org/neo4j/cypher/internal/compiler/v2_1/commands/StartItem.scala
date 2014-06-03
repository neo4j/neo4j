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
package org.neo4j.cypher.internal.compiler.v2_1.commands

import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_1.mutation._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.PlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v2_1.Argument

trait NodeStartItemIdentifiers extends StartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTNode)
}

trait RelationshipStartItemIdentifiers extends StartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTRelationship)
}

abstract class StartItem(val identifierName: String, val args: Seq[Argument])
  extends TypeSafe with AstNode[StartItem] {
  def mutating: Boolean
  def producerType: String = getClass.getSimpleName
  def identifiers: Seq[(String, CypherType)]
  def arguments: Seq[Argument] = args ++ identifiers.map(x => Arguments.IntroducedIdentifier(x._1))
}

trait ReadOnlyStartItem {
  def mutating = false

  // AstNode implementations
  def children: Seq[AstNode[_]] = Nil
  def symbolTableDependencies:Set[String] = Set.empty
  def rewrite(f: (Expression) => Expression):this.type = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression))) with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(query)))
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(query),
    Arguments.LegacyIndex(idxName)))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

trait Hint

sealed abstract class SchemaIndexKind

case object AnyIndex extends SchemaIndexKind
case object UniqueIndex extends SchemaIndexKind

trait QueryExpression {
  def expression:Expression
}
case class SingleQueryExpression(expression:Expression) extends QueryExpression
case class ManyQueryExpression(expression:Expression) extends QueryExpression

case class SchemaIndex(identifier: String, label: String, property: String, kind: SchemaIndexKind, query: Option[QueryExpression])
  extends StartItem(identifier, query.map(q => Arguments.LegacyExpression(q.expression)).toSeq :+ Arguments.Index(label, property))
  with ReadOnlyStartItem with Hint with NodeStartItemIdentifiers

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByIdOrEmpty(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class NodeByLabel(varName: String, label: String)
  extends StartItem(varName, Seq(Arguments.LabelName(label)))
  with ReadOnlyStartItem with Hint with NodeStartItemIdentifiers

case class AllNodes(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with NodeStartItemIdentifiers

case class AllRelationships(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with RelationshipStartItemIdentifiers

case class LoadCSV(withHeaders: Boolean, url: Expression, identifier: String, fieldTerminator: Option[String]) extends StartItem(identifier, Seq.empty)
  with ReadOnlyStartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> (if (withHeaders) CTMap else CTCollection(CTAny)))
}

case class Unwind(expression: Expression, identifier: String) extends StartItem(identifier, Seq(Arguments.IntroducedIdentifier(identifier)))
  with ReadOnlyStartItem {
  def identifiers: Seq[(String, CypherType)] = Seq(identifierName -> CTAny)
}

//We need to wrap the inner classes to be able to have two different rewrite methods
abstract class UpdatingStartItem(val updateAction: UpdateAction, name: String) extends StartItem(name, Seq(Arguments.UpdateActionName(name))) {

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
