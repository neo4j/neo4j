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
package org.neo4j.cypher.internal.compiler.v2_2.mutation

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}

import scala.collection.JavaConverters._

case class DeleteEntityAction(elementToDelete: Expression)
  extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState) = {
    elementToDelete(context)(state) match {
      case n: Node => delete(n, state)
      case r: Relationship => delete(r, state)
      case null =>
      case p:Path => p.iterator().asScala.foreach( pc => delete(pc, state))
      case x => throw new CypherTypeException("Expression `" + elementToDelete.toString() + "` yielded `" + x.toString + "`. Don't know how to delete that.")
    }
    Iterator(context)
  }

  private def delete(x: PropertyContainer, state: QueryState) {

    x match {
      case n: Node if !state.query.nodeOps.isDeleted(n)=>
        state.query.nodeOps.delete(n)

      case r: Relationship if !state.query.relationshipOps.isDeleted(r) =>
        state.query.relationshipOps.delete(r)

      case _ =>
        // Entity is already deleted. No need to do anything
    }
  }

  def identifiers: Seq[(String, CypherType)] = Nil

  def rewrite(f: (Expression) => Expression) = DeleteEntityAction(elementToDelete.rewrite(f))

  def children = Seq(elementToDelete)

  def symbolTableDependencies = elementToDelete.symbolTableDependencies

  def localEffects(symbols: SymbolTable) = elementToDelete match {
    case i: Identifier => symbols.identifiers(i.entityName) match {
      case _: NodeType         => Effects(DeletesNode, WritesNodes, WritesAnyLabel, WritesAnyNodeProperty)
      case _: RelationshipType => Effects(DeletesRelationship, WritesRelationships, WritesAnyRelationshipProperty)
      case _: PathType         => Effects(DeletesRelationship, WritesRelationships, WritesAnyRelationshipProperty,
                                          DeletesNode, WritesNodes, WritesAnyLabel, WritesAnyNodeProperty)
      case _                   => Effects((AllWriteEffects | Effects(DeletesNode, DeletesRelationship)).effectsSet)
    }
    case _ => Effects((AllWriteEffects | Effects(DeletesNode, DeletesRelationship)).effectsSet)
  }
}
