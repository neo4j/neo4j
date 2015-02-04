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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.graphdb.{Relationship, Node}

case class KeysFunction(nodeOrRelationshipExpr: Expression) extends NullInNullOutExpression(nodeOrRelationshipExpr) {

  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = value match {
    case node: Node        => node.getId
    case rel: Relationship => rel.getId
    case x => throw new CypherTypeException("Expected `%s` to be a node or relationship, but it was ``".format(nodeOrRelationshipExpr, x.getClass.getSimpleName))
  }

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = nodeOrRelationshipExpr(ctx) match {
    case n: Node =>
      val queryCtx: QueryContext = state.query
      queryCtx.getPropertiesForNode(n.getId).map { case (v) =>
        queryCtx.getPropertyKeyName(v.toInt)
      }.toList
    case rel: Relationship =>
      val queryCtx: QueryContext = state.query
      queryCtx.getPropertiesForRelationship(rel.getId).map { case (v) =>
        queryCtx.getPropertyKeyName(v.toInt)
      }.toList
    case null =>  null
    case _    =>  throw new CypherTypeException("keys() expected a Node or Relationship but it was called with something else")
  }

  def rewrite(f: (Expression) => Expression) = f(KeysFunction(nodeOrRelationshipExpr.rewrite(f)))

  def arguments = Seq(nodeOrRelationshipExpr)

  def symbolTableDependencies = nodeOrRelationshipExpr.symbolTableDependencies

  protected def calculateType(symbols: SymbolTable) = {
    CTCollection(CTString)
  }

  override def localEffects = Effects.READS_NODES

}
