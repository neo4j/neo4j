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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{RelatedTo, SingleNode}
import commands.expressions.Expression
import commands.values.{UnresolvedProperty, KeyToken}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import pipes.QueryState
import spi.QueryContext
import org.neo4j.graphdb.Node
import collection.Map

class PatternNode(key: String, val labels: Seq[KeyToken] = Seq.empty, val properties: Map[KeyToken, Expression] = Map.empty)
  extends PatternElement(key) {

  def this(node: SingleNode) = {
    this(node.name, node.labels, node.properties.map {
      case (k, e) => (UnresolvedProperty(k), e)
    })
  }

  def canUseThis(graphNodeId: Long, state: QueryState, ctx: ExecutionContext): Boolean =
    nodeHasLabels(graphNodeId, state.query) &&
    nodeHasProperties(graphNodeId, ctx)(state)

  val relationships = scala.collection.mutable.Set[PatternRelationship]()

  def getPRels(history: Seq[MatchingPair]): Seq[PatternRelationship] = relationships.filterNot(r => history.exists(_.matches(r))).toSeq

  def getGraphRelationships(node: Node, pRel: PatternRelationship, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] =
    pRel.getGraphRelationships(this, node, state, f)

  def relateTo(key: String, other: PatternNode, relType: Seq[String], dir: SemanticDirection,
               props: Map[String, Expression] = Map.empty): PatternRelationship = {
    val relProps = props.map { case (k,v) => UnresolvedProperty(k)->v }.toMap
    val rel = new PatternRelationship(key, this, other, relType, relProps, dir)
    relationships.add(rel)
    other.relationships.add(rel)
    rel
  }

  def relateTo(key: String, other: PatternNode, r: RelatedTo): PatternRelationship =
    relateTo(key, other, r.relTypes, r.direction, r.properties)

  def relateViaVariableLengthPathTo(pathName: String,
                                    end: PatternNode,
                                    minHops: Option[Int],
                                    maxHops: Option[Int],
                                    relType: Seq[String],
                                    dir: SemanticDirection,
                                    collectionOfRels: Option[String],
                                    props: Map[String, Expression] = Map.empty): PatternRelationship = {
    val relProps = props.map { case (k,v) => UnresolvedProperty(k)->v }.toMap
    val rel = new VariableLengthPatternRelationship(pathName, this, end, collectionOfRels, minHops, maxHops, relType, relProps, dir)
    relationships.add(rel)
    end.relationships.add(rel)
    rel
  }

  override def toString = String.format("PatternNode[key=%s]", key)

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  path: Seq[PatternElement]) {
    if (!path.contains(this)) {
      val moreData = visitNode(this, data)

      relationships.
        filter(shouldFollow).
        foreach(r => r.traverse(shouldFollow, visitNode, visitRelationship, moreData, this, path :+ this))
    }
  }

  private def nodeHasLabels(graphNodeId: Long, ctx: QueryContext): Boolean = {
    val expectedLabels: Seq[Option[Int]] = labels.map(_.getOptId(ctx))

    expectedLabels.forall {
      case None          => false
      case Some(labelId) => ctx.isLabelSetOnNode(labelId, graphNodeId)
    }
  }

  private def nodeHasProperties(graphNodeId: Long, execCtx: ExecutionContext)(implicit state: QueryState): Boolean =
    properties.forall {
    case (token, expression) =>
      val propertyId = token.getOptId(state.query)
      if (propertyId.isEmpty) false // The property doesn't exist in the graph
      else {
        val value = state.query.nodeOps.getProperty(graphNodeId, propertyId.get)
        val expectedValue = expression(execCtx)
        value == expectedValue
      }
  }
}
