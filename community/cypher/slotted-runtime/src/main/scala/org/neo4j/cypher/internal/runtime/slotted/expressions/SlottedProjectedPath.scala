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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions.startNode
import org.neo4j.cypher.operations.PathValueBuilder
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

object SlottedProjectedPath {

  trait Projector {
    def apply(context: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder

    /**
     * The Expressions used in this Projector
     */
    def arguments: Seq[Expression]
  }

  object nilProjector extends Projector {
    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = builder

    override def arguments: Seq[Expression] = Seq.empty
  }

  case class singleNodeProjector(node: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val nodeValue = node.apply(ctx, state)
      builder.addNode(nodeValue)
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = Seq(node) ++ tailProjector.arguments
  }

  case class singleRelationshipWithKnownTargetProjector(rel: Expression, target: Expression, tailProjector: Projector)
      extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relValue = rel.apply(ctx, state)
      val nodeValue = target.apply(ctx, state)
      builder.addRelationship(relValue)
      builder.addNode(nodeValue)
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = Seq(rel, target) ++ tailProjector.arguments
  }

  case class singleIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {

      tailProjector(ctx, state, addIncoming(rel.apply(ctx, state), state, builder))
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class singleOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addOutgoing(rel.apply(ctx, state), state, builder))

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class singleUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addUndirected(rel.apply(ctx, state), state, builder))

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiIncomingRelationshipWithKnownTargetProjector(
    rel: Expression,
    node: Expression,
    tailProjector: Projector
  ) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      rel.apply(ctx, state) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(
            builder,
            list,
            (b, v) => {
              b.addIncoming(v)
              b
            }
          )
          aggregated.addRelationship(list.last())
          aggregated.addNode(node.apply(ctx, state))
          tailProjector(ctx, state, builder)

        case _: ListValue => tailProjector(ctx, state, builder)
        case x if x eq NO_VALUE =>
          builder.addNoValue()
          tailProjector(ctx, state, builder)
        case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
      }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiOutgoingRelationshipWithKnownTargetProjector(
    rel: Expression,
    node: Expression,
    tailProjector: Projector
  ) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      rel.apply(ctx, state) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(
            builder,
            list,
            (b, v) => {
              b.addOutgoing(v)
              b
            }
          )
          aggregated.addRelationship(list.last())
          aggregated.addNode(node.apply(ctx, state))
          tailProjector(ctx, state, aggregated)

        case _: ListValue => tailProjector(ctx, state, builder)
        case x if x eq NO_VALUE =>
          builder.addNoValue()
          tailProjector(ctx, state, builder)
        case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
      }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiUndirectedRelationshipWithKnownTargetProjector(
    rel: Expression,
    node: Expression,
    tailProjector: Projector
  ) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      rel.apply(ctx, state) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(
            builder,
            list,
            (b, v) => {
              b.addUndirected(v)
              b
            }
          )
          aggregated.addRelationship(list.last())
          aggregated.addNode(node.apply(ctx, state))
          tailProjector(ctx, state, aggregated)

        case _: ListValue => tailProjector(ctx, state, builder)
        case x if x eq NO_VALUE =>
          builder.addNoValue()
          tailProjector(ctx, state, builder)
        case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
      }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      builder.addMultipleIncoming(relListValue)
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      builder.addMultipleOutgoing(relListValue)
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      builder.addMultipleUndirected(relListValue)
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class quantifiedPathProjector(groupVariables: Seq[Expression], toNode: Expression, tailProjector: Projector)
      extends Projector {

    override def apply(ctx: ReadableRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {

      /**
       * Given a pattern: (from)( (a)-[r1]-(b)-[r2]-(c) ){2,2}(to)
       *
       * We could have:
       * variables = [ [from/a_1, a_2] [r1_1,r1_2] , [b_1, b_2] , [r2_1,r2_2] ]
       * toNode = to
       *
       * And therefore:
       * path = [from/a_1 , r1_1, b_1, r2_1, a_2, r1_2, b_2, r2_2, to]
       */
      val values = groupVariables.map(
        _.apply(ctx, state) match {
          case value: ListValue   => value
          case x if x eq NO_VALUE => VirtualValues.EMPTY_LIST
          case value              => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
        }
      )
      val patternSize = values.size
      val repetitions = values.head.size()
      if (patternSize * repetitions > 0) {
        var repetitionOffset = 0
        while (repetitionOffset < repetitions) {
          // Skip first node, was added by previous Projector
          var patternOffset = if (repetitionOffset == 0) 1 else 0
          while (patternOffset < patternSize) {
            if (patternOffset % 2 == 0) {
              // Node
              builder.addNode(values(patternOffset).value(repetitionOffset))
            } else {
              // Relationship
              builder.addRelationship(values(patternOffset).value(repetitionOffset))
            }
            patternOffset += 1
          }
          repetitionOffset += 1
        }
        builder.addNode(toNode.apply(ctx, state))
      }
      tailProjector(ctx, state, builder)
    }

    override def arguments: Seq[Expression] = groupVariables ++ Seq(toNode) ++ tailProjector.arguments
  }

  private def addIncoming(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: VirtualRelationshipValue =>
      builder.addRelationship(r)
      builder.addNode(startNode(r, state.query, state.cursors.relationshipScanCursor))
      builder

    case x if x eq NO_VALUE =>
      builder.addNoValue()
      builder
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addOutgoing(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: VirtualRelationshipValue =>
      builder.addOutgoing(r)
      builder
    case x if x eq NO_VALUE =>
      builder.addNoValue()
      builder
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addUndirected(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: VirtualRelationshipValue =>
      builder.addUndirected(r)
      builder

    case x if x eq NO_VALUE =>
      builder.addNoValue()
      builder
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addAllExceptLast(
    builder: PathValueBuilder,
    list: ListValue,
    f: (PathValueBuilder, AnyValue) => PathValueBuilder
  ) = {
    var aggregated = builder
    val size = list.size()
    var i = 0
    while (i < size - 1) {
      // we know these relationships have already loaded start and end relationship
      // so we should not use CypherFunctions::[start,end]Node to look them up
      aggregated = f(aggregated, list.value(i))
      i += 1
    }
    aggregated
  }
}

/*
 Expressions for materializing new paths (used by ronja)

 These expressions cannot be generated by the user directly
 */
case class SlottedProjectedPath(projector: SlottedProjectedPath.Projector) extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    projector(row, state, new PathValueBuilder(state.query, state.cursors.relationshipScanCursor)).build()

  override def arguments: Seq[Expression] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def children: Seq[AstNode[_]] = projector.arguments
}
