/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

import scala.collection.Map

case class CreateNode(key: String, properties: Map[String, Expression], labels: Seq[KeyToken])
  extends UpdateAction
  with GraphElementPropertyFunctions
  with CollectionSupport {

  def localEffects(symbols: SymbolTable) = {
    val writeEffects = if (labels.isEmpty) Effects(WritesAnyNode) else Effects(WritesNodesWithLabels(labels.map(_.name).toSet))
    val propertyEffects = properties.values.foldLeft(Effects())(_ | _.effects(symbols))
    val labelEffects = Effects(labels.map(kt => WritesNodesWithLabels(kt.name)).toSet[Effect])

    writeEffects | propertyEffects | labelEffects
  }

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {
    def fromAnyToLiteral(x: Map[String, Any]): Map[String, Expression] = x.map {
      case (k, v:Any) => k -> Literal(v)
    }

    def createNodeWithPropertiesAndLabels(props: Map[String, Expression]): ExecutionContext = {
      val node = state.query.createNode()
      setProperties(node, props, context, state)

      val queryCtx = state.query
      val labelIds = labels.map(_.getOrCreateId(state.query))
      if (labelIds.nonEmpty)
        queryCtx.setLabelsOnNode(node.getId, labelIds.iterator)

      val newContext = context.newWith(key -> node)
      newContext
    }

    def isParametersMap(m: Map[String, Expression]) = properties.size == 1 && properties.head._1 == "*"

    /*
     Parameters coming in from the outside in queries using parameters like this:

     CREATE (n {param})

     This parameter can either be a collection of maps, or a single map. Cypher creates one node per incoming map.

     This is encoded using a map containing the expression that when applied will produce the incoming maps.
     */
    if (isParametersMap(properties)) {
      val singleMapExpression: Expression = properties.head._2

      val maps = makeTraversable(singleMapExpression(context)(state))

      maps.toIterator.map {
        case untyped: Map[_, _] => {
          //We want to use the same code to actually create nodes and properties as a normal expression would, so we
          //encode the incoming Map[String,Any] to a Map[String, Literal] wrapping the values.
          val m: Map[String, Expression] = fromAnyToLiteral(untyped.asInstanceOf[Map[String, Any]])

          createNodeWithPropertiesAndLabels(m)
        }
        case _ => throw new CypherTypeException("Parameter provided for node creation is not a Map")
      }
    } else {
      Iterator(createNodeWithPropertiesAndLabels(properties))
    }
  }

  def identifiers = Seq(key -> CTNode)

  override def children = properties.map(_._2).toSeq ++ labels.flatMap(_.children)

  override def rewrite(f: (Expression) => Expression): CreateNode =
    CreateNode(key, properties.rewrite(f), labels.map(_.typedRewrite[KeyToken](f)))

  override def symbolTableDependencies: Set[String] =
    properties.symboltableDependencies ++ labels.flatMap(_.symbolTableDependencies)
}
