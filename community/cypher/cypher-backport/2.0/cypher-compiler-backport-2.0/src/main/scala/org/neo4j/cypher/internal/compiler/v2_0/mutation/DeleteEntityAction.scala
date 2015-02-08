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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.Expression
import pipes.QueryState
import symbols._
import org.neo4j.cypher.CypherTypeException
import org.neo4j.graphdb.{PropertyContainer, Path, Relationship, Node}
import collection.JavaConverters._
import org.neo4j.kernel.impl.core.NodeManager

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
    val nodeManager: NodeManager = state.graphDatabaseAPI.getDependencyResolver.resolveDependency(classOf[NodeManager])

    x match {
      case n: Node if !nodeManager.isDeleted(n) =>
        state.query.nodeOps.delete(n)

      case r: Relationship if !nodeManager.isDeleted(r) =>
        state.query.relationshipOps.delete(r)

      case _ => // Entity is already deleted. No need to do anything
    }
  }

  def identifiers: Seq[(String, CypherType)] = Nil

  def rewrite(f: (Expression) => Expression) = DeleteEntityAction(elementToDelete.rewrite(f))

  def children = Seq(elementToDelete)

  def symbolTableDependencies = elementToDelete.symbolTableDependencies
}
