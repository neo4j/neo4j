/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, RelTypeName, RelationshipPattern, LabelName, NodePattern}

case class UpdateGraph(nodePatterns: Seq[CreateNodePattern] = Seq.empty,
                       relPatterns: Seq[CreateRelationshipPattern] = Seq.empty) {

  def ++(other: UpdateGraph) = copy(nodePatterns = nodePatterns ++ other.nodePatterns)

  def isEmpty = this == UpdateGraph.empty

  def nonEmpty = !isEmpty

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    nodePatterns.map(p => p.nodeName -> p.labels.toSet).toMap

  def labels: Set[LabelName] = nodePatterns.flatMap(_.labels).toSet

  def relTypes: Set[RelTypeName] = relPatterns.map(_.relType).toSet

  def overlaps(qg: QueryGraph) =
    qg.patternNodes.nonEmpty &&
      nonEmpty &&
      (nodeOverlap(qg) || relationshipOverlap(qg))

  def nodeOverlap(qg: QueryGraph) = {
    qg.patternNodes.exists(p => qg.allKnownLabelsOnNode(p).isEmpty) || //MATCH ()?
      nodePatterns.exists(p => p.labels.isEmpty) || //CREATE()?
      (qg.patternNodeLabels.values.flatten.toSet intersect labels).nonEmpty // CREATE(:A:B) MATCH(:B:C)?
  }

  def relationshipOverlap(qg: QueryGraph) = {
    //MATCH ()-[]->()?
    qg.patternRelationships.exists(_.types.nonEmpty) ||
      // CREATE ()-[:R]->() MATCH ()-[:R]-()?
      (qg.patternRelationships.flatMap(_.types.toSet) intersect relTypes).nonEmpty
  }


  def addNodePatterns(nodePatterns: CreateNodePattern*): UpdateGraph = {

    copy(nodePatterns = (this.nodePatterns ++ nodePatterns).distinct)
  }

  def addRelPatterns(relationships: CreateRelationshipPattern*): UpdateGraph =
    copy(relPatterns = (this.relPatterns ++ relationships).distinct)
}

object UpdateGraph {
  val empty = UpdateGraph()
}

case class CreateNodePattern(nodeName: IdName, labels: Seq[LabelName], properties: Option[Expression])
case class CreateRelationshipPattern(relName: IdName, startNode: IdName, relType: RelTypeName, endNode: IdName, properties: Option[Expression])
