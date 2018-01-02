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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsRelationships, WritesRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{SetAction, GraphElementPropertyFunctions}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InvalidSemanticsException, InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.collection.PrefetchingIterator
import org.neo4j.kernel.api.properties.Property

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
 * Merge when both end-points are known, find all relationships of the given
 * type in the given direction between the two end-points.
 *
 * This is done by checking both nodes and starts from any non-dense node of the two.
 * If both nodes are dense, we find the degree of each and expand from the smaller of the two
 */
case class MergeIntoPipe(source: Pipe,
                         fromName: String,
                         relName: String,
                         toName: String,
                         dir: SemanticDirection,
                         typ: String,
                         props: Map[KeyToken, Expression],
                         onCreateActions: Seq[SetAction],
                         onMatchActions: Seq[SetAction])(val estimatedCardinality: Option[Double] = None)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions {
  self =>

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val typeId = state.query.getOrCreateRelTypeId(typ)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        val toNode = getRowNode(row, toName)

        if (fromNode == null || toNode == null)
          Iterator(row.newWith2(relName, null, toName, toNode))
        else {
          val relationships = findRelationships(fromNode, toNode, typeId)(state, row)

          if (relationships.isEmpty) {
            val relationship = if (dir == SemanticDirection.INCOMING)
              state.query.createRelationship(toNode.getId, fromNode.getId, typeId)
            else
              state.query.createRelationship(fromNode.getId, toNode.getId, typeId)
            setPropertiesOnRelationship(row, relationship, state, props)
            val newContext = row.newWith2(relName, relationship, toName, toNode)
            onCreateActions.foreach(_.exec(newContext, state))
            Iterator(newContext)
          } else {
            relationships.map { relationship =>
              val newContext = row.newWith2(relName, relationship, toName, toNode)
              onMatchActions.foreach(_.exec(newContext, state))
              newContext
            }
          }
        }
    }
  }

  /**
   * Finds all relationships connecting fromNode and toNode.
   */
  private def findRelationships(fromNode: Node, toNode: Node, typeId: Int)(implicit queryState: QueryState, execution: ExecutionContext): Iterator[Relationship] = {
    val query = queryState.query
    val fromNodeIsDense = query.nodeIsDense(fromNode.getId)
    val toNodeIsDense = query.nodeIsDense(toNode.getId)

    //if both nodes are dense, start from the one with the lesser degree
    if (fromNodeIsDense && toNodeIsDense) {
      //check degree and iterate from the node with smaller degree
      val fromDegree = query.nodeGetDegree(fromNode.getId, dir, typeId)
      if (fromDegree == 0) {
        return Iterator.empty
      }

      val toDegree = getDegree(toNode, typeId, dir.reversed, query)
      if (toDegree == 0) {
        return Iterator.empty
      }

      relIterator(fromNode, toNode, fromDegree < toDegree, typeId)
    }
    // iterate from a non-dense node
    else if (fromNodeIsDense)
      relIterator(fromNode, toNode, preserveDirection = false, typeId)
    else
      relIterator(fromNode, toNode, preserveDirection = true, typeId)
  }

  private def relIterator(fromNode: Node, toNode: Node, preserveDirection: Boolean,
                          typeId: Int)(implicit queryState: QueryState, execution: ExecutionContext) = {
    val query = queryState.query
    val (start, localDirection, end) = if (preserveDirection) (fromNode, dir, toNode) else (toNode, dir.reversed, fromNode)
    val relationships = query.getRelationshipsForIds(start, localDirection, Some(Seq(typeId)))

    new PrefetchingIterator[Relationship] {

      private def hasCorrectProperties(rel: Relationship): Boolean = props.forall { case (key, expression) =>
        val expressionValue = expression(execution)(queryState)
        val propertyKeyId = key.getOptId(query)
        propertyKeyId.exists { id =>
          toComparableProperty(query.relationshipOps.getProperty(rel.getId, id)) == expressionValue
        }
      }

      override def fetchNextOrNull(): Relationship = {
        while (relationships.hasNext) {
          val rel = relationships.next()
          val other = rel.getOtherNode(start)
          if (end == other) {
            if (hasCorrectProperties(rel)) return rel
          }
        }
        null
      }
    }.asScala
  }

  /*
   * Properties can contain arrays which are not comparable with ==
   */
  private def toComparableProperty(property: Any) = property match {
    case a: Array[_] => a.toVector
    case o => o
  }

  private def getDegree(node: Node, typeId: Int, direction: SemanticDirection, query: QueryContext) = {
    query.nodeGetDegree(node.getId, direction, typeId)
  }

  @inline
  private def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null => null
      case value => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  def planDescriptionWithoutCardinality =
    source.planDescription.andThen(this.id, "Merge(Into)", identifiers, ExpandExpression(fromName, relName, Seq(typ), toName, dir))

  val symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  override def localEffects = {
    val effects = Effects(ReadsRelationships, WritesRelationships)
    val onCreateEffects = onCreateActions.foldLeft(effects) {
      case (acc, action) => acc | action.localEffects(symbols)
    }
    onMatchActions.foldLeft(onCreateEffects) {
      case (acc, action) => acc | action.localEffects(symbols)
    }
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def setPropertiesOnRelationship(row: ExecutionContext, relationship: Relationship, state: QueryState,
                                          properties: Map[KeyToken, Expression]): Unit = {
    properties.foreach { case (keyToken, expression) =>
      val expressionValue = makeValueNeoSafe(expression(row)(state))
      if (expressionValue == null) {
        throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for ${keyToken.name}")
      }
      state.query.relationshipOps.setProperty(relationship.getId, keyToken.getOrCreateId(state.query), expressionValue)
    }
  }
}
