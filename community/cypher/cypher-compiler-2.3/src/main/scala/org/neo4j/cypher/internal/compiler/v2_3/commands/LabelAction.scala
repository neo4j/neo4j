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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{WritesNodesWithLabels, Effect, Effects}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{SetAction}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.graphdb.Node

sealed abstract class LabelOp

case object LabelSetOp extends LabelOp
case object LabelRemoveOp extends LabelOp

//TODO: Should take single label
case class LabelAction(entity: Expression, labelOp: LabelOp, labels: Seq[KeyToken])
  extends SetAction with CollectionSupport {

  def localEffects(ignored: SymbolTable) = Effects(labels.map(l => WritesNodesWithLabels(l.name)).toSet[Effect])

  def children = labels.flatMap(_.children) :+ entity

  def rewrite(f: (Expression) => Expression) =
    LabelAction(entity.rewrite(f), labelOp, labels.map(_.typedRewrite[KeyToken](f)))

  def exec(context: ExecutionContext, state: QueryState) = {
    val node      = CastSupport.castOrFail[Node](entity(context)(state))
    val queryCtx  = state.query

    labelOp match {
     case LabelSetOp =>
       val labelIds = labels.map(_.getOrCreateId(state.query))
       queryCtx.setLabelsOnNode(node.getId, labelIds.iterator)
     case LabelRemoveOp =>
        val labelIds = labels.flatMap(_.getOptId(state.query))
        queryCtx.removeLabelsFromNode(node.getId, labelIds.iterator)
    }

    Iterator(context)
  }

  def identifiers = Seq.empty

  def symbolTableDependencies = entity.symbolTableDependencies ++ labels.flatMap(_.symbolTableDependencies)
}
