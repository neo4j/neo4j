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

import expressions.{Identifier, Literal, Expression}
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.cypher.internal.mutation.{GraphElementPropertyFunctions, UpdateAction}
import scala.Long
import collection.Map
import org.neo4j.graphdb.{DynamicRelationshipType, Node}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.helpers.CollectionSupport


abstract class StartItem(val identifierName: String) extends TypeSafe {
  def mutating = false
}

trait ReadOnlyStartItem extends TypeSafe {
  def assertTypes(symbols: SymbolTable) {}

  def symbolTableDependencies = Set()
}

case class RelationshipById(varName: String, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class RelationshipByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class RelationshipByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeByIndex(varName: String, idxName: String, key: Expression, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeByIndexQuery(varName: String, idxName: String, query: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class NodeById(varName: String, expression: Expression)
  extends StartItem(varName) with ReadOnlyStartItem

case class AllNodes(columnName: String)
  extends StartItem(columnName) with ReadOnlyStartItem

case class AllRelationships(columnName: String)
  extends StartItem(columnName) with ReadOnlyStartItem

case class CreateNodeStartItem(key: String, props: Map[String, Expression])
  extends StartItem(key)
  with Mutator
  with UpdateAction
  with GraphElementPropertyFunctions
  with CollectionSupport {
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

  def identifier2 = Seq(key -> NodeType())

  def filter(f: (Expression) => Boolean): Seq[Expression] = props.values.flatMap(_.filter(f)).toSeq

  def rewrite(f: (Expression) => Expression): UpdateAction = CreateNodeStartItem(key, rewrite(props, f))

  def assertTypes(symbols: SymbolTable) {
    checkTypes(props, symbols)
  }

  def symbolTableDependencies = symbolTableDependencies(props)
}

case class CreateRelationshipStartItem(key: String,
                                       from: (Expression, Map[String, Expression]),
                                       to: (Expression, Map[String, Expression]),
                                       typ: String, props: Map[String, Expression])
  extends StartItem(key)
  with Mutator
  with UpdateAction
  with GraphElementPropertyFunctions {
  private lazy val relationshipType = DynamicRelationshipType.withName(typ)

  def filter(f: (Expression) => Boolean): Seq[Expression] = from._1.filter(f) ++ props.values.flatMap(_.filter(f))

  def rewrite(f: (Expression) => Expression) = CreateRelationshipStartItem(key, (f(from._1), from._2), (f(to._1), to._2), typ, props.map(mapRewrite(f)))

  def exec(context: ExecutionContext, state: QueryState) = {
    val f = from._1(context).asInstanceOf[Node]
    val t = to._1(context).asInstanceOf[Node]
    val relationship = f.createRelationshipTo(t, relationshipType)
    state.createdRelationships.increase()
    setProperties(relationship, props, context, state)
    context.put(key, relationship)
    Stream(context)
  }

  def identifier2 = Seq(key-> RelationshipType())

  def assertTypes(symbols: SymbolTable) {
    checkTypes(from._2, symbols)
    checkTypes(to._2, symbols)
    checkTypes(props, symbols)
  }

  def symbolTableDependencies = (from._2.flatMap(_._2.symbolTableDependencies) ++
                                to._2.flatMap(_._2.symbolTableDependencies) ++
                                props.flatMap(_._2.symbolTableDependencies)).toSet
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

