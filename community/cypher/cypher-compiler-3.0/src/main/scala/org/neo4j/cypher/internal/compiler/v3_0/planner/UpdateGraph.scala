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
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, Identifier, LabelName, PathExpression, RelTypeName}

case class UpdateGraph(mutatingPatterns: Seq[MutatingPattern] = Seq.empty) {



  def ++(other: UpdateGraph) = copy(mutatingPatterns = mutatingPatterns ++ other.mutatingPatterns)

  def isEmpty = this == UpdateGraph.empty

  def nonEmpty = !isEmpty

  def nodePatterns = mutatingPatterns.collect {
    case p: CreateNodePattern => p
  }

  def relPatterns: Seq[CreateRelationshipPattern] = mutatingPatterns.collect {
    case p: CreateRelationshipPattern => p
  }

  def deleteExpressions = mutatingPatterns.collect {
    case p: DeleteExpression => p
  }

  def deleteIdentifiers = (deleteExpressions flatMap {
    case DeleteExpression(identifier:Identifier, _) => Seq(IdName.fromIdentifier(identifier))
    case DeleteExpression(PathExpression(e), _) => e.dependencies.map(IdName.fromIdentifier)
  }).toSet

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    nodePatterns.map(p => p.nodeName -> p.labels.toSet).toMap

  def labels: Set[LabelName] = nodePatterns.flatMap(_.labels).toSet

  def relTypes: Set[RelTypeName] = relPatterns.map(_.relType).toSet

  def overlaps(qg: QueryGraph) =
    qg.patternNodes.nonEmpty &&
      nonEmpty &&
      (nodeOverlap(qg) || relationshipOverlap(qg) || deleteOverlap(qg))

  private def nodeOverlap(qg: QueryGraph) = {
    qg.patternNodes.exists(p => qg.allKnownLabelsOnNode(p).isEmpty) || //MATCH ()?
      (qg.patternNodeLabels.values.flatten.toSet intersect labels).nonEmpty // CREATE(:A:B) MATCH(:B:C)?
  }

  def relationshipOverlap(qg: QueryGraph) = {
    //CREATE () MATCH ()-->()
    (relPatterns.nonEmpty && qg.patternRelationships.nonEmpty) && (
      //MATCH ()-[]->()?
      qg.patternRelationships.exists(_.types.isEmpty) ||
        // CREATE ()-[:R]->() MATCH ()-[:R]-()?
        (qg.patternRelationships.flatMap(_.types.toSet) intersect relTypes).nonEmpty ||
        // CREATE (a)-[:R1]->(b) MATCH (a)-[:R2]-(b)?
        (qg.patternRelationships.flatMap(r => Set(r.nodes._1, r.nodes._2)) intersect nodePatterns.map(_.nodeName).toSet)
          .nonEmpty
      )
  }

  def setLabelOverlap(qg: QueryGraph): Boolean = {
    val labelsToSet = mutatingPatterns.collect {
      case SetLabelPattern(_, labels) => labels
    }.flatten
    qg.patternNodes.exists(p => qg.allKnownLabelsOnNode(p).intersect(labelsToSet).nonEmpty)
  }

  def deleteOverlap(qg: QueryGraph): Boolean = {
    val identifiersToDelete = deleteIdentifiers
    val identifiersToRead = qg.patternNodes ++ qg.patternRelationships.map(_.name)
    (identifiersToRead intersect identifiersToDelete).nonEmpty
  }

  def addNodePatterns(nodePatterns: CreateNodePattern*): UpdateGraph =
    copy(mutatingPatterns = (this.mutatingPatterns ++ nodePatterns).distinct)

  def addRelPatterns(relationships: CreateRelationshipPattern*): UpdateGraph =
    copy(mutatingPatterns = (this.mutatingPatterns ++ relationships).distinct)

  def addSetLabel(setLabelPatterns: SetLabelPattern*): UpdateGraph =
    copy(mutatingPatterns = this.mutatingPatterns ++ setLabelPatterns)

  def addDeleteExpression(deleteExpressions: DeleteExpression*) =
    copy(mutatingPatterns = this.mutatingPatterns ++ deleteExpressions)
}

object UpdateGraph {
  val empty = UpdateGraph()
}

trait MutatingPattern

case class CreateNodePattern(nodeName: IdName, labels: Seq[LabelName], properties: Option[Expression]) extends MutatingPattern

case class CreateRelationshipPattern(relName: IdName, leftNode: IdName, relType: RelTypeName, rightNode: IdName,
                                     properties: Option[Expression], direction: SemanticDirection) extends  MutatingPattern {
  assert(direction != SemanticDirection.BOTH)

  def startNode = inOrder._1

  def endNode = inOrder._2

  def inOrder =  if (direction == SemanticDirection.OUTGOING) (leftNode, rightNode) else (rightNode, leftNode)
}

case class SetLabelPattern(idName: IdName, labels: Seq[LabelName]) extends MutatingPattern

case class DeleteExpression(expression: Expression, forced: Boolean) extends MutatingPattern
