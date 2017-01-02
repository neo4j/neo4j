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
package org.neo4j.cypher.internal.compiler.v3_0.mutation

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{ContainerIndex, Expression, Variable}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphdb

import scala.collection.JavaConverters._

case class DeleteEntityAction(elementToDelete: Expression, forced: Boolean)
  extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState) = {
    elementToDelete(context)(state) match {
      case n: graphdb.Node => delete(n, state, forced)
      case r: graphdb.Relationship => delete(r, state, forced)
      case null =>
      case p: graphdb.Path => p.iterator().asScala.foreach(pc => delete(pc, state, forced))
      case x => throw new CypherTypeException("Expression `" + elementToDelete.toString() + "` yielded `" + x.toString + "`. Don't know how to delete that.")
    }
    Iterator(context)
  }

  private def delete(x: graphdb.PropertyContainer, state: QueryState, forced: Boolean) {
    x match {
      case n: graphdb.Node if !state.query.nodeOps.isDeletedInThisTx(n) =>
        if (forced) state.query.detachDeleteNode(n)
        else state.query.nodeOps.delete(n)

      case r: graphdb.Relationship if !state.query.relationshipOps.isDeletedInThisTx(r) =>
        state.query.relationshipOps.delete(r)

      case _ =>
      // Entity is already deleted. No need to do anything
    }
  }

  def variables: Seq[(String, CypherType)] = Nil

  def rewrite(f: (Expression) => Expression) =
    DeleteEntityAction(elementToDelete.rewrite(f), forced)

  def children = Seq(elementToDelete)

  def symbolTableDependencies = elementToDelete.symbolTableDependencies

  def localEffects(symbols: SymbolTable) = elementToDelete match {
    case i: Variable => effectsFromCypherType(symbols.variables(i.entityName))
    case ContainerIndex(i: Variable, _) => symbols.variables(i.entityName) match {
      case ListType(innerType) => effectsFromCypherType(innerType)
    }
    // There could be a nested map/collection expression here, so we'll
    // just say that we don't know what type the entity has
    case _ =>
      Effects(DeletesNode, DeletesRelationship)
  }

  private def effectsFromCypherType(cypherType: CypherType) = cypherType match {
    case _: NodeType         => Effects(DeletesNode)
    case _: RelationshipType => Effects(DeletesRelationship)
    case _: PathType         => Effects(DeletesNode, DeletesRelationship)
    case _                   => Effects(DeletesNode, DeletesRelationship)
  }
}
