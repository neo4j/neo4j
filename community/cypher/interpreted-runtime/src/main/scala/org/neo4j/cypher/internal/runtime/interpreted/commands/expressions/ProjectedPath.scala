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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.Projector
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.IterableHasAsScala

object ProjectedPath {

  type Projector = (ReadableRow, PathValueBuilder) => PathValueBuilder

  object nilProjector extends Projector {
    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = builder
  }

  case class singleNodeProjector(node: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = {

      tailProjector(ctx, builder.addNode(ctx.getByName(node)))
    }
  }

  case class singleIncomingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addIncomingRelationship(ctx.getByName(rel)))
  }

  case class singleRelationshipWithKnownTargetProjector(rel: String, target: String, tailProjector: Projector)
      extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addRelationship(ctx.getByName(rel)).addNode(ctx.getByName(target)))
  }

  case class singleOutgoingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addOutgoingRelationship(ctx.getByName(rel)))
  }

  case class singleUndirectedRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addUndirectedRelationship(ctx.getByName(rel)))
  }

  case class multiIncomingRelationshipProjector(rels: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addIncomingRelationships(ctx.getByName(rels)))
  }

  case class multiOutgoingRelationshipProjector(rels: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addOutgoingRelationships(ctx.getByName(rels)))
  }

  case class multiUndirectedRelationshipProjector(rels: String, tailProjector: Projector) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, builder.addUndirectedRelationships(ctx.getByName(rels)))
  }

  case class multiIncomingRelationshipWithKnownTargetProjector(
    rels: String,
    node: String,
    tailProjector: Projector
  ) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = {
      ctx.getByName(rels) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(builder, list, (b, v) => b.addIncomingRelationship(v)).addRelationship(
            list.last()
          ).addNode(
            ctx.getByName(node)
          )
          tailProjector(ctx, aggregated)

        case _: ListValue       => tailProjector(ctx, builder)
        case x if x eq NO_VALUE => builder.addNoValue()
        case value              => throw new CypherTypeException(s"Expected ListValue but got ${value}")
      }
    }
  }

  case class multiOutgoingRelationshipWithKnownTargetProjector(
    rels: String,
    node: String,
    tailProjector: Projector
  ) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = {
      ctx.getByName(rels) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(builder, list, (b, v) => b.addOutgoingRelationship(v)).addRelationship(
            list.last()
          ).addNode(
            ctx.getByName(node)
          )
          tailProjector(ctx, aggregated)

        case _: ListValue       => tailProjector(ctx, builder)
        case x if x eq NO_VALUE => builder.addNoValue()
        case value              => throw new CypherTypeException(s"Expected ListValue but got ${value}")
      }
    }
  }

  case class multiUndirectedRelationshipWithKnownTargetProjector(
    rels: String,
    node: String,
    tailProjector: Projector
  ) extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = {
      ctx.getByName(rels) match {
        case list: ListValue if list.nonEmpty() =>
          val aggregated = addAllExceptLast(builder, list, (b, v) => b.addUndirectedRelationship(v)).addRelationship(
            list.last()
          ).addNode(ctx.getByName(node))
          tailProjector(ctx, aggregated)

        case _: ListValue       => tailProjector(ctx, builder)
        case x if x eq NO_VALUE => builder.addNoValue()
        case value              => throw new CypherTypeException(s"Expected ListValue but got ${value}")
      }
    }
  }

  case class quantifiedPathProjector(variables: Seq[String], toNode: String, tailProjector: Projector)
      extends Projector {

    def apply(ctx: ReadableRow, builder: PathValueBuilder): PathValueBuilder = {
      val listValues = variables.map(ctx.getByName(_) match {
        case value: ListValue   => value
        case x if x eq NO_VALUE => VirtualValues.EMPTY_LIST
        case value              => throw new CypherTypeException(s"Expected ListValue but got ${value}")
      }).map(_.asScala).transpose.flatten

      if (listValues.nonEmpty) {
        // Skip first element via `.tail`, since that is equal to the node added in the last Projector
        for ((entity, position) <- listValues.tail.zipWithIndex) {
          if (position % 2 == 0) {
            // Relationship
            builder.addRelationship(entity)
          } else {
            // Node
            builder.addNode(entity)
          }
        }
        builder.addNode(ctx.getByName(toNode))
      }
      tailProjector(ctx, builder)
    }
  }

  private def addAllExceptLast(
    builder: PathValueBuilder,
    list: ListValue,
    f: (PathValueBuilder, AnyValue) => PathValueBuilder
  ): PathValueBuilder = {
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
 Expressions for materializing new paths

 These expressions cannot be generated by the user directly
 */
case class ProjectedPath(projector: Projector) extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    projector(row, new PathValueBuilder(state)).result()
  }

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(this)
}
