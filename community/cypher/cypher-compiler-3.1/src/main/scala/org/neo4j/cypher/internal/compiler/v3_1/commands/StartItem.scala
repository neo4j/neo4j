/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_1.mutation._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_1.symbols.{SymbolTable, TypeSafe}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

trait NodeStartItemVariables extends StartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTNode)
}

trait RelationshipStartItemVariables extends StartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTRelationship)
}

abstract class StartItem(val variableName: String, val arguments: Seq[Argument])
  extends TypeSafe with EffectfulAstNode[StartItem] {
  def producerType: String = getClass.getSimpleName
  def variables: Seq[(String, CypherType)]
  def mutating: Boolean = effects.containsWrites
  def effects: Effects = effects(SymbolTable(variables.toMap))
}

trait ReadOnlyStartItem {
  // AstNode implementations
  def children: Seq[AstNode[_]] = Nil
  def symbolTableDependencies:Set[String] = Set.empty
  def rewrite(f: (Expression) => Expression): this.type = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression))) with ReadOnlyStartItem with RelationshipStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllRelationships)
}

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with RelationshipStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllRelationships)
}

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(query)))
  with ReadOnlyStartItem with RelationshipStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllRelationships)
}

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with NodeStartItemVariables with Hint {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes)
}

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(query),
    Arguments.LegacyIndex(idxName)))
  with ReadOnlyStartItem with NodeStartItemVariables with Hint {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes)
}

trait Hint

sealed abstract class SchemaIndexKind

case object AnyIndex extends SchemaIndexKind
case object UniqueIndex extends SchemaIndexKind

// TODO Unify with Sargable

trait QueryExpression[+T] {
  def expression: T
  def map[R](f: T => R): QueryExpression[R]
}

case class ScanQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: (T) => R) = ScanQueryExpression(f(expression))
}

case class SingleQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: (T) => R) = SingleQueryExpression(f(expression))
}

case class ManyQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: (T) => R) = ManyQueryExpression(f(expression))
}

case class RangeQueryExpression[T](expression: T) extends QueryExpression[T] {
  override def map[R](f: (T) => R) = RangeQueryExpression(f(expression))
}

case class SchemaIndex(variable: String, label: String, property: String, kind: SchemaIndexKind, query: Option[QueryExpression[Expression]])
  extends StartItem(variable, query.map(q => Arguments.LegacyExpression(q.expression)).toIndexedSeq :+ Arguments.Index(label, property))
  with ReadOnlyStartItem with Hint with NodeStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsNodesWithLabels(label), ReadsGivenNodeProperty(property))
}

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes)
}

case class NodeByIdOrEmpty(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes)
}

case class NodeByLabel(varName: String, label: String)
  extends StartItem(varName, Seq(Arguments.LabelName(label)))
  with ReadOnlyStartItem with Hint with NodeStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsNodesWithLabels(label))
}

case class AllNodes(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with NodeStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllNodes)
}

case class AllRelationships(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with RelationshipStartItemVariables {
  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllRelationships)
}

case class LoadCSV(withHeaders: Boolean, url: Expression, variable: String, fieldTerminator: Option[String]) extends StartItem(variable, Seq.empty)
  with ReadOnlyStartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> (if (withHeaders) CTMap else CTList(CTAny)))
  override def localEffects(symbols: SymbolTable) = Effects()
}

case class Unwind(expression: Expression, variable: String) extends StartItem(variable, Seq())
  with ReadOnlyStartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTAny)
  override def localEffects(symbols: SymbolTable) = Effects()
}

//We need to wrap the inner classes to be able to have two different rewrite methods
abstract class UpdatingStartItem(val updateAction: UpdateAction, name: String) extends StartItem(name, Seq(Arguments.UpdateActionName(name))) {
  override def children = Seq(updateAction)
  override def symbolTableDependencies = updateAction.symbolTableDependencies

  def localEffects(symbols: SymbolTable) = updateAction.localEffects(symbols)
  def variables: Seq[(String, CypherType)] = updateAction.variables
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

case class MergeNodeStartItem(inner: MergeNodeAction) extends UpdatingStartItem(inner, inner.variable) {
  override def rewrite(f: (Expression) => Expression) = MergeNodeStartItem(inner.rewrite(f))
}

/** NodeById that throws exception if no node is found */
object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}
