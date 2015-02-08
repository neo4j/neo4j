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

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Expression
import collection.Map
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{QueryState}
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{SymbolTable, RelationshipType}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext

case class CreateRelationship(key: String,
                                       from: (Expression, Map[String, Expression]),
                                       to: (Expression, Map[String, Expression]),
                                       typ: String, props: Map[String, Expression])
  extends UpdateAction
  with GraphElementPropertyFunctions {

  override def children = props.map(_._2).toSeq ++ Seq(from._1, to._1) ++ from._2.map(_._2) ++ to._2.map(_._2)

  override def rewrite(f: (Expression) => Expression) = {
      val newFrom = (f(from._1), from._2.map(mapRewrite(f)))
      val newTo = (f(to._1), to._2.map(mapRewrite(f)))
      val newProps = props.map(mapRewrite(f))

      CreateRelationship(key, newFrom, newTo, typ, newProps)
    }

  def exec(context: ExecutionContext, state: QueryState) = {
    val f = from._1(context)(state).asInstanceOf[Node]
    val t = to._1(context)(state).asInstanceOf[Node]
    val relationship = state.query.createRelationship(f, t, typ)
    state.createdRelationships.increase()
    setProperties(relationship, props, context, state)
    context.put(key, relationship)
    Stream(context)
  }

  private def mapRewrite(f: (Expression) => Expression)(kv: (String, Expression)): (String, Expression) = kv match {
    case (k, v) => (k, f(v))
  }

  def identifiers = Seq(key-> RelationshipType())

  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    throwIfSymbolsMissing(from._2, symbols)
    throwIfSymbolsMissing(to._2, symbols)
    throwIfSymbolsMissing(props, symbols)
  }

  override def symbolTableDependencies: Set[String] = (from._2.flatMap(_._2.symbolTableDependencies) ++
                                to._2.flatMap(_._2.symbolTableDependencies) ++
                                props.flatMap(_._2.symbolTableDependencies)).toSet
}
