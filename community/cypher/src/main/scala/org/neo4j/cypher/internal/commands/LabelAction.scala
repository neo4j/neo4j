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
package org.neo4j.cypher.internal.commands

import expressions.Expression
import org.neo4j.cypher.internal.mutation.{GraphElementPropertyFunctions, UpdateAction}
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.helpers.{LabelSupport, CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.parser.v2_0.LabelSet


sealed abstract class LabelOp

case object LabelAdd extends LabelOp
case object LabelDel extends LabelOp

case class LabelAction(entity: Expression, labelOp: LabelOp, labelSet: LabelSet)
  extends UpdateAction with GraphElementPropertyFunctions with CollectionSupport with LabelSupport {

  def children = Seq(entity, labelSet)
  def rewrite(f: (Expression) => Expression) = LabelAction(entity.rewrite(f), labelOp, labelSet.rewrite(f))

  def exec(context: ExecutionContext, state: QueryState) = {
    val node      = CastSupport.erasureCastOrFail[Node](entity(context))
    val queryCtx  = state.queryContext
    val labelIds: Iterable[Long] = getLabelsAsLongs(context, labelSet.expr)

    labelOp match {
      case LabelAdd => queryCtx.addLabelsToNode(node.getId, labelIds)
      case LabelDel => queryCtx.removeLabelsFromNode(node.getId, labelIds)
    }

    Stream(context)
  }

  def identifiers = Seq.empty

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    entity.throwIfSymbolsMissing(symbols)
    labelSet.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = entity.symbolTableDependencies ++ labelSet.symbolTableDependencies
}