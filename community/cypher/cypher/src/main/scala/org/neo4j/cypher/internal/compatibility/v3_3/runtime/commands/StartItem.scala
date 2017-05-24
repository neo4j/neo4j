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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.NodeById.pos
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.symbols.TypeSafe
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.QueryExpression
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast.UnsignedDecimalIntegerLiteral
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

case class RelationshipById(varName: String, expression: Expression, args: Seq[Argument])
  extends StartItem(varName, args) with ReadOnlyStartItem with RelationshipStartItemVariables

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
  with ReadOnlyStartItem with RelationshipStartItemVariables

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
  with ReadOnlyStartItem with RelationshipStartItemVariables

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
  with ReadOnlyStartItem with NodeStartItemVariables with Hint

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
  with ReadOnlyStartItem with NodeStartItemVariables with Hint

trait Hint

sealed abstract class SchemaIndexKind

case object AnyIndex extends SchemaIndexKind
case object UniqueIndex extends SchemaIndexKind

case class SchemaIndex(variable: String, label: String, properties: Seq[String], kind: SchemaIndexKind,
                       query: Option[QueryExpression[Expression]], args: Seq[Argument])
  extends StartItem(variable, args)
  with ReadOnlyStartItem with Hint with NodeStartItemVariables

case class NodeById(varName: String, expression: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
  with ReadOnlyStartItem with NodeStartItemVariables

case class NodeByIdOrEmpty(varName: String, expression: Expression, args: Seq[Argument])
  extends StartItem(varName, args)
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
  private val pos = InputPosition(-1,-1,-1)
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id),
                                                       id.map(i => Arguments.Expression(UnsignedDecimalIntegerLiteral(i.toString)(pos))))
}

object RelationshipById {
  private val pos = InputPosition(-1,-1,-1)
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id),
                                                               id.map(i => Arguments.Expression(UnsignedDecimalIntegerLiteral(i.toString)(pos))))
}
