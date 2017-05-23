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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.symbols.TypeSafe
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

trait NodeStartItemVariables extends StartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTNode)
}

trait RelationshipStartItemVariables extends StartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTRelationship)
}

abstract class StartItem(val variableName: String, val arguments: Seq[Argument])
  extends TypeSafe with AstNode[StartItem] {
  def producerType: String = getClass.getSimpleName
  def variables: Seq[(String, CypherType)]
}

trait ReadOnlyStartItem {
  // AstNode implementations
  def children: Seq[AstNode[_]] = Nil
  def symbolTableDependencies:Set[String] = Set.empty
  def rewrite(f: (Expression) => Expression): this.type = this
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression))) with ReadOnlyStartItem with RelationshipStartItemVariables

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with RelationshipStartItemVariables

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(query)))
  with ReadOnlyStartItem with RelationshipStartItemVariables

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(expression),
    Arguments.LegacyIndex(idxName),
    Arguments.LegacyExpression(key)))
  with ReadOnlyStartItem with NodeStartItemVariables with Hint

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName, Seq(
    Arguments.LegacyExpression(query),
    Arguments.LegacyIndex(idxName)))
  with ReadOnlyStartItem with NodeStartItemVariables with Hint

trait Hint

sealed abstract class SchemaIndexKind

case object AnyIndex extends SchemaIndexKind
case object UniqueIndex extends SchemaIndexKind
// TODO Unify with Sargable
trait QueryExpression[+T] {
  def expressions: Seq[T]
  def map[R](f: T => R): QueryExpression[R]
}

trait SingleExpression[+T] {
  def expression: T

  def expressions = Seq(expression)
}

case class ScanQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = ScanQueryExpression(f(expression))
}

case class SingleQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = SingleQueryExpression(f(expression))
}

case class ManyQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  def map[R](f: T => R) = ManyQueryExpression(f(expression))
}

case class RangeQueryExpression[T](expression: T) extends QueryExpression[T] with SingleExpression[T] {
  override def map[R](f: T => R) = RangeQueryExpression(f(expression))
}

case class CompositeQueryExpression[T](inner: Seq[QueryExpression[T]]) extends QueryExpression[T] {
  def map[R](f: T => R) = CompositeQueryExpression(inner.map(_.map(f)))

  override def expressions: Seq[T] = inner.flatMap(_.expressions)
}

case class SchemaIndex(variable: String, label: String, properties: Seq[String], kind: SchemaIndexKind, query: Option[QueryExpression[Expression]])
  extends StartItem(variable, query.map(q => q.expressions.map(Arguments.LegacyExpression)).getOrElse(Seq.empty) :+ Arguments.Index(label, properties))
  with ReadOnlyStartItem with Hint with NodeStartItemVariables

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemVariables

case class NodeByIdOrEmpty(varName: String, expression: Expression)
  extends StartItem(varName, Seq(Arguments.LegacyExpression(expression)))
  with ReadOnlyStartItem with NodeStartItemVariables

case class NodeByLabel(varName: String, label: String)
  extends StartItem(varName, Seq(Arguments.LabelName(label)))
  with ReadOnlyStartItem with Hint with NodeStartItemVariables

case class AllNodes(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with NodeStartItemVariables

case class AllRelationships(columnName: String) extends StartItem(columnName, Seq.empty)
  with ReadOnlyStartItem with RelationshipStartItemVariables

case class LoadCSV(withHeaders: Boolean, url: Expression, variable: String, fieldTerminator: Option[String]) extends StartItem(variable, Seq.empty)
  with ReadOnlyStartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> (if (withHeaders) CTMap else CTList(CTAny)))
}

case class Unwind(expression: Expression, variable: String) extends StartItem(variable, Seq())
  with ReadOnlyStartItem {
  def variables: Seq[(String, CypherType)] = Seq(variableName -> CTAny)
}

/** NodeById that throws exception if no node is found */
object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}
