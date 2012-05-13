/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.cypher.internal.mutation.{GraphElementPropertyFunctions, UpdateAction}
import scala.Long
import collection.Map
import org.neo4j.graphdb.{DynamicRelationshipType, Node}
import org.neo4j.cypher.internal.symbols._


abstract sealed class StartItem(val identifierName: String) {
  def mutating = false
}

abstract class RelationshipStartItem(id: String) extends StartItem(id)

abstract class NodeStartItem(id: String) extends StartItem(id)

case class RelationshipById(varName: String, expression: Expression) extends RelationshipStartItem(varName)

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression) extends RelationshipStartItem(varName)

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression) extends RelationshipStartItem(varName)

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression) extends NodeStartItem(varName)

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression) extends NodeStartItem(varName)

case class NodeById(varName: String, expression: Expression) extends NodeStartItem(varName)

case class AllNodes(columnName: String) extends NodeStartItem(columnName)

case class AllRelationships(columnName: String) extends RelationshipStartItem(columnName)

case class CreateNodeStartItem(key: String, props: Map[String, Expression])
  extends NodeStartItem(key)
  with Mutator
  with UpdateAction
  with GraphElementPropertyFunctions
  with IterableSupport {
  def exec(context: ExecutionContext, state: QueryState) = {
    val db = state.db
    if (props.size == 1 && props.head._1 == "*") {
      makeTraversable(props.head._2(context)).map(x => {
        val m: Map[String, Expression] = x.asInstanceOf[Map[String, Any]].map {
          case (k, v) => (k -> Literal(v))
        }
        val node = db.createNode()
        state.createdNodes.increase()
        setProperties(node, m, context, state)
        context.newWith(key -> node)
      })
    } else {
      val node = db.createNode()
      state.createdNodes.increase()
      setProperties(node, props, context, state)

      Stream(context.newWith(key -> node))
    }
  }

  def dependencies = propDependencies(props)

  def identifier = Seq(Identifier(key, NodeType()))

  def filter(f: (Expression) => Boolean): Seq[Expression] = props.values.flatMap(_.filter(f)).toSeq

  def rewrite(f: (Expression) => Expression): UpdateAction = CreateNodeStartItem(key, rewrite(props, f))
}

case class CreateRelationshipStartItem(key: String, from: Expression, to: Expression, typ: String, props: Map[String, Expression])
  extends NodeStartItem(key)
  with Mutator
  with UpdateAction
  with GraphElementPropertyFunctions {
  private lazy val relationshipType = DynamicRelationshipType.withName(typ)

  def dependencies = from.dependencies(NodeType()) ++ to.dependencies(NodeType()) ++ propDependencies(props)

  def filter(f: (Expression) => Boolean): Seq[Expression] = from.filter(f) ++ props.values.flatMap(_.filter(f))

  def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(key, f(from), f(to), typ, props.map(mapRewrite(f)))

  def exec(context: ExecutionContext, state: QueryState) = {
    val f = from(context).asInstanceOf[Node]
    val t = to(context).asInstanceOf[Node]
    val relationship = f.createRelationshipTo(t, relationshipType)
    state.createdRelationships.increase()
    setProperties(relationship, props, context, state)
    context.put(key, relationship)
    Stream(context)
  }

  def identifier = Seq(Identifier(key, RelationshipType()))
}

trait Mutator extends StartItem {
  override def mutating = true

  def mapRewrite(f: (Expression) => Expression)(kv: (String, Expression)): (String, Expression) = kv match {
    case (k, v) => (k, f(v))
  }
}

object NodeById {
  def apply(varName: String, id: Long*) = new NodeById(varName, Literal(id))
}

object RelationshipById {
  def apply(varName: String, id: Long*) = new RelationshipById(varName, Literal(id))
}

