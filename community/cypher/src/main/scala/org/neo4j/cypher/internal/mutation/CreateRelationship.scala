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

import org.neo4j.cypher.internal.commands.expressions.Expression
import collection.Map
import org.neo4j.cypher.internal.pipes.{QueryState}
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.symbols.{SymbolTable, RelationshipType}
import org.neo4j.cypher.internal.ExecutionContext

case class RelationshipEndpoint(node: Expression, props: Map[String, Expression], labels: Expression, bare: Boolean)
  extends GraphElementPropertyFunctions {
  def rewrite(f: (Expression) => Expression): RelationshipEndpoint =
    RelationshipEndpoint(node.rewrite(f), props.mapValues(_.rewrite(f)), labels.rewrite(f), bare)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    props.throwIfSymbolsMissing(symbols)
    labels.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies: Set[String] =
    props.symboltableDependencies ++ labels.symbolTableDependencies
}

case class CreateRelationship(key: String,
                              from: RelationshipEndpoint,
                              to: RelationshipEndpoint,
                              typ: String, props: Map[String, Expression])
extends UpdateAction
  with GraphElementPropertyFunctions {

  override def children =
    props.map(_._2).toSeq ++ Seq(from.node, to.node) ++ from.props.map(_._2) ++ to.props.map(_._2) :+ to.labels :+ from.labels

  override def rewrite(f: (Expression) => Expression) = {
      val newFrom = from.rewrite(f)
      val newTo = to.rewrite(f)
      val newProps = props.mapValues(_.rewrite(f))

      CreateRelationship(key, newFrom, newTo, typ, newProps)
    }

  def exec(context: ExecutionContext, state: QueryState) = {
    val f = from.node(context).asInstanceOf[Node]
    val t = to.node(context).asInstanceOf[Node]
    val relationship = state.queryContext.createRelationship(f, t, typ)
    setProperties(relationship, props, context, state)
    context.put(key, relationship)
    Stream(context)
  }

  def identifiers = Seq(key-> RelationshipType())

  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    from.throwIfSymbolsMissing(symbols)
    to.throwIfSymbolsMissing(symbols)
    props.throwIfSymbolsMissing(symbols)
  }

  override def symbolTableDependencies: Set[String] =
    (props.flatMap(_._2.symbolTableDependencies)).toSet ++
    from.symbolTableDependencies ++ to.symbolTableDependencies
}
