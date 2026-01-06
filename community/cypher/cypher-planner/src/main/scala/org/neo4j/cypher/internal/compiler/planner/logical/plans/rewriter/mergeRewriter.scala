/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MergeInto
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.mutable.ArrayBuffer

case class mergeRewriter(supportsFastExpandInto: Boolean) extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = {
    instance.apply(input)
  }

  override val innerRewriter: Rewriter = Rewriter.lift {
    case m @ Merge(
        Expand(
          arg: Argument,
          from,
          dir,
          Seq(relType),
          Some(to),
          Some(rel),
          ExpandInto
        ),
        _,
        _,
        _,
        _,
        _
      ) if supportsFastExpandInto && isRewritable(from, dir, relType, to, rel, m) =>
      val RelationshipProperties = RelationshipPropertiesForName(rel)
      m match {
        case Merge(_, _, _, RelationshipProperties(onMatch), RelationshipProperties(onCreate), _) =>
          MergeInto(
            arg,
            rel,
            from,
            dir,
            relType,
            to,
            onMatch,
            onCreate
          )(SameId(m.id))
        case _ => m
      }

// TODO: uncomment with proper resolution of IND-150

//    case m @ Merge(
//        NodeUniqueIndexSeek(
//          idName,
//          label,
//          properties,
//          MergeUniqueSeekExpression(seekExpressions),
//          args,
//          indexOrder,
//          indexType,
//          _
//        ),
//        _,
//        _,
//        _,
//        _,
//        _
//      ) if seekExpressions.length == properties.length =>
//      val NodeProperties = NodePropertiesForName(idName)
//      m match {
//        case Merge(_, _, _, NodeProperties(onMatch), NodeProperties(onCreate), _) =>
//          MergeUniqueNode(
//            idName,
//            label,
//            properties,
//            seekExpressions,
//            args,
//            indexOrder,
//            indexType,
//            onMatch,
//            onCreate
//          )(SameId(m.id))
//        case _ => m
//      }
  }

//  private object MergeUniqueSeekExpression {
//
//    def unapply(in: QueryExpression[Expression]): Option[Seq[Expression]] = in match {
//      case SingleQueryExpression(expression) => Some(Seq(expression))
//      case CompositeQueryExpression(inner) if inner.forall(_.isInstanceOf[SingleQueryExpression[_]]) =>
//        Some(inner.map(_.asInstanceOf[SingleQueryExpression[Expression]].expression))
//      case _ => None
//    }
//  }

  private case class RelationshipPropertiesForName(relName: LogicalVariable) {

    def unapply(in: Seq[SetMutatingPattern]): Option[Seq[(PropertyKeyName, Expression)]] = {
      val list = new ArrayBuffer[(PropertyKeyName, Expression)]()
      val it = in.iterator
      while (it.hasNext) {
        it.next() match {
          case SetRelationshipPropertyPattern(variable, key, value)
            if variable == relName && value.isConstantForQuery => list.append(key -> value)
          case SetRelationshipPropertiesPattern(variable, items)
            if variable == relName && items.forall(_._2.isConstantForQuery) => list.appendAll(items)
          case _ => return None
        }
      }
      Some(list.toSeq)
    }
  }

//  private case class NodePropertiesForName(relName: LogicalVariable) {
//
//    def unapply(in: Seq[SetMutatingPattern]): Option[Seq[(PropertyKeyName, Expression)]] = {
//      val list = new ArrayBuffer[(PropertyKeyName, Expression)]()
//      val it = in.iterator
//      while (it.hasNext) {
//        it.next() match {
//          case SetNodePropertyPattern(variable, key, value)
//            if variable == relName && value.isConstantForQuery => list.append(key -> value)
//          case SetNodePropertiesPattern(variable, items)
//            if variable == relName && items.forall(_._2.isConstantForQuery) => list.appendAll(items)
//          case _ => return None
//        }
//      }
//      Some(list.toSeq)
//    }
//  }

  private def isRewritable(
    from: LogicalVariable,
    direction: SemanticDirection,
    relType: RelTypeName,
    to: LogicalVariable,
    rel: LogicalVariable,
    m: Merge
  ): Boolean = {
    if (m.createRelationships.length == 1) {
      val cr = m.createRelationships.head
      m.nodesToLock.contains(from) &&
      m.nodesToLock.contains(to) &&
      m.createNodes.isEmpty &&
      cr.leftNode == from &&
      cr.rightNode == to &&
      cr.direction == direction &&
      cr.relType == relType &&
      cr.variable == rel
    } else {
      false
    }
  }

  private val instance: Rewriter = bottomUp(innerRewriter)
}
