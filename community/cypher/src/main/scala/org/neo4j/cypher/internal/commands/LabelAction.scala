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

import expressions.{LabelValue, Expression, Collection}
import org.neo4j.cypher.internal.mutation.{GraphElementPropertyFunctions, UpdateAction}
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.helpers.{IsCollection, CollectionSupport}
import scala.collection.JavaConverters._

sealed abstract class LabelOp

case object LabelSet extends LabelOp { override def toString = "=" }
case object LabelAdd extends LabelOp { override def toString = "+=" }
case object LabelDel extends LabelOp { override def toString = "-=" }

case class LabelAction(entity: Expression, labelOp: LabelOp, labelSetExpr: Expression)
  extends UpdateAction with GraphElementPropertyFunctions with CollectionSupport {

  def children = Seq(entity, labelSetExpr)

  def rewrite(f: (Expression) => Expression) = LabelAction(entity.rewrite(f), labelOp, labelSetExpr.rewrite(f))

  def exec(context: ExecutionContext, state: QueryState) = {
    def getNodeFromExpression: Node = {
      entity(context) match {
        case x: Node => x
        case x =>
          throw new CypherTypeException("Expected %s to be a node, but it was :`%s`".format(entity, x))
      }
    }

    val node = getNodeFromExpression
    val queryCtx = state.query

    val labelVals: Iterable[LabelValue] = labelSetExpr(context) match {
      case l: LabelValue => Iterable(l)
      case c: Iterable[_] => c.map {
        case (l: LabelValue) => l
        case _ => throw new CypherTypeException("Encountered label collection with non-label values")
      }
    }

    val labelIds = labelVals.map { labelVal => queryCtx.getOrCreateLabelId(labelVal.name) }

    queryCtx.addLabelsToNode(node, labelIds.asJava)

    Stream(context)
  }

  def identifiers = Seq.empty

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    entity.throwIfSymbolsMissing(symbols)
    labelSetExpr.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = entity.symbolTableDependencies ++ labelSetExpr.symbolTableDependencies
}