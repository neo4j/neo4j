/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import commands.values.KeyToken
import pipes.QueryState
import symbols._
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.helpers.ThisShouldNotHappenError

case class DeletePropertyAction(element: Expression, propertyKey: KeyToken)
  extends UpdateAction {

  def exec(context: ExecutionContext, state: QueryState) = {
    propertyKey.getOptId(state.query) match {
      case Some(propertyKeyId) =>
        element(context)(state) match {
          case n: Node =>
            if (state.query.nodeOps.hasProperty(n, propertyKeyId)) {
              state.query.nodeOps.removeProperty(n, propertyKeyId)
            }

          case r: Relationship  =>
            if (state.query.relationshipOps.hasProperty(r, propertyKeyId)) {
              state.query.relationshipOps.removeProperty(r, propertyKeyId)
            }

          case _ =>
            throw new ThisShouldNotHappenError("Andres", "This should be a node or a relationship")
        }

      case None =>
    }
    Iterator(context)
  }

  def identifiers = Nil

  def children = Seq(element)

  def rewrite(f: (Expression) => Expression) = DeletePropertyAction(element.rewrite(f), propertyKey.rewrite(f))

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    element.evaluateType(MapType(), symbols)
  }

  def symbolTableDependencies = element.symbolTableDependencies
}
