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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.commands.expressions.{Literal, Expression}
import org.neo4j.cypher.internal.helpers.{LabelSupport, CollectionSupport}
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.symbols.{SymbolTable, NodeType}
import collection.Map
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.values.LabelValue

case class CreateNode(key: String, properties: Map[String, Expression], labels: Seq[LabelValue], bare: Boolean = true)
  extends UpdateAction
  with GraphElementPropertyFunctions
  with CollectionSupport {



  def exec(context: ExecutionContext, state: QueryState) = {
    def fromAnyToLiteral(x: Map[String, Any]): Map[String, Expression] = x.map {
      case (k, v:Any) => (k -> Literal(v))
    }

    def createNodeWithPropertiesAndLabels(props: Map[String, Expression]): ExecutionContext = {
      val node = state.query.createNode()
      setProperties(node, props, context, state)

      val queryCtx = state.query
      val labelIds = labels.map(_.id(state))
      queryCtx.setLabelsOnNode(node.getId, labelIds)

      val newContext = context.newWith(key -> node)
      newContext
    }

    def isParametersMap(m: Map[String, Expression]) = properties.size == 1 && properties.head._1 == "*"

    /*
     Parameters coming in from the outside in queries using parameters like this:

     CREATE n = {param}

     This parameter can either be a collection of maps, or a single map. Cypher creates one node per incoming map.

     This is encoded using a map containing the expression that when applied will produce the incoming maps.
     */
    if (isParametersMap(properties)) {
      val singleMapExpression: Expression = properties.head._2

      val maps: Iterable[Any] = makeTraversable(singleMapExpression(context)(state))

      maps.map {
        case untyped: Map[_, _] => {
          //We want to use the same code to actually create nodes and properties as a normal expression would, so we
          //encode the incoming Map[String,Any] to a Map[String, Literal] wrapping the values.
          val m: Map[String, Expression] = fromAnyToLiteral(untyped.asInstanceOf[Map[String, Any]])

          createNodeWithPropertiesAndLabels(m)
        }
      }
    } else {
      Stream(createNodeWithPropertiesAndLabels(properties))
    }
  }

  def identifiers = Seq(key -> NodeType())

  override def children = properties.map(_._2).toSeq ++ labels.flatMap(_.children)

  override def rewrite(f: (Expression) => Expression): CreateNode =
    CreateNode(key, properties.rewrite(f), labels.map(_.typedRewrite[LabelValue](f)), bare)

  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    properties throwIfSymbolsMissing symbols
    for (label <- labels)
      label throwIfSymbolsMissing symbols
  }

  override def symbolTableDependencies: Set[String] =
    properties.symboltableDependencies ++ labels.flatMap(_.symbolTableDependencies)
}