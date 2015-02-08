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
package org.neo4j.cypher.internal.compiler.v1_9.mutation

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Literal, Expression}
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{SymbolTable, NodeType}
import collection.Map
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext

case class CreateNode(key: String, props: Map[String, Expression])
  extends UpdateAction
  with GraphElementPropertyFunctions
  with CollectionSupport {
  def exec(context: ExecutionContext, state: QueryState) = {
    if (props.size == 1 && props.head._1 == "*") {
      val expression = props.head._2
      makeTraversable(expression(context)(state)).map(x => {
        val m: Map[String, Expression] = x.asInstanceOf[Map[String, Any]].map {
          case (k, v) => (k -> Literal(v))
        }
        val node = state.query.createNode()
        state.createdNodes.increase()
        setProperties(node, m, context, state)
        context.newWith(key -> node)
      })
    } else {
      val node = state.query.createNode()
      state.createdNodes.increase()
      setProperties(node, props, context, state)

      Stream(context.newWith(key -> node))
    }
  }

  def identifiers = Seq(key -> NodeType())

  override def children = props.map(_._2).toSeq

  override def rewrite(f: (Expression) => Expression): CreateNode = CreateNode(key, rewrite(props, f))

  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    throwIfSymbolsMissing(props, symbols)
  }

  override def symbolTableDependencies: Set[String] = symbolTableDependencies(props)
}
