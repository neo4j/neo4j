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
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, Identifier, LabelName, MapExpression, PathExpression, PropertyKeyName, RelTypeName}

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

  def identifiersToDelete = (deleteExpressions flatMap {
    case DeleteExpression(identifier:Identifier, _) => Seq(IdName.fromIdentifier(identifier))
    case DeleteExpression(PathExpression(e), _) => e.dependencies.map(IdName.fromIdentifier)
  }).toSet

  def removeLabelPatterns = mutatingPatterns.collect {
    case p: RemoveLabelPattern => p
  }

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    nodePatterns.map(p => p.nodeName -> p.labels.toSet).toMap

  def createLabels: Set[LabelName] = nodePatterns.flatMap(_.labels).toSet

  def createProperties = {
    //all created properties
    val properties = nodePatterns.flatMap(_.properties)
    //CREATE ()
    if (properties.isEmpty) CreatesNoPropertyKeys
    else {
      val knownProp: Seq[Seq[(PropertyKeyName, Expression)]] = properties.collect {
        case MapExpression(props) => props
      }
      //all prop keys are known, CREATE ({prop1:1, prop2:2})
      if (knownProp.size == properties.size) CreatesKnownPropertyKeys(knownProp.flatMap(_.map(s => s._1)).toSet)
      //props created are not known, e.g. CREATE ({props})
      else CreatesUnknownPropertyKeys
    }
  }

  def labelsToRemove: Set[LabelName] = removeLabelPatterns.flatMap(_.labels).toSet

  def labelsToRemoveForNode(idName: IdName): Set[LabelName] = removeLabelPatterns.collect {
    case RemoveLabelPattern(n, labels) if n == idName => labels
  }.flatten.toSet

  def relTypes: Set[RelTypeName] = relPatterns.map(_.relType).toSet

  def updatesNodes = nodePatterns.nonEmpty || removeLabelPatterns.nonEmpty

  def overlaps(qg: QueryGraph) =
    qg.patternNodes.nonEmpty &&
      nonEmpty &&
      (nodeOverlap(qg) || relationshipOverlap(qg) ||
        deleteOverlap(qg) || removeLabelOverlap(qg) || setLabelOverlap(qg) || setPropertyOverlap(qg))

  private def nodeOverlap(qg: QueryGraph) = {
    val propsToCreate = createProperties
    qg.patternNodes.exists(p => {
      val readProps = qg.allKnownPropertiesOnNode(p).map(_.propertyKey)

      qg.allKnownLabelsOnNode(p).isEmpty && readProps.isEmpty || //MATCH ()?
        readProps.exists(propsToCreate.overlaps) //MATCH ({prop:..}) CREATE ({prop:..})
    }
    ) ||
      (qg.patternNodeLabels.values.flatten.toSet intersect createLabels).nonEmpty // CREATE(:A:B) MATCH(:B:C)?
  }

  private def removeLabelOverlap(qg: QueryGraph) = {
    removeLabelPatterns.exists {
      case RemoveLabelPattern(removeId, labelsToRemove) =>
        //does any other identifier match on the labels I am deleting?
        //MATCH (a:BAR)..(b) REMOVE b:BAR
        labelsToRemove.exists(l => {
          val otherLabelsRead = qg.patternNodes.filterNot(_ == removeId).flatMap(qg.allKnownLabelsOnNode)
          otherLabelsRead(l)
        })
    }
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

  def setPropertyOverlap(qg: QueryGraph) = setNodePropertyOverlap(qg) || setRelPropertyOverlap(qg)

  private def setNodePropertyOverlap(qg: QueryGraph): Boolean = {
    val propertiesToSet = mutatingPatterns.collect {
      case SetNodePropertyPattern(_, key, _) => key
    }.toSet

    val propertiesToRead = qg.allKnownNodeProperties.map(_.propertyKey)

    (propertiesToRead intersect propertiesToSet).nonEmpty
  }

  private def setRelPropertyOverlap(qg: QueryGraph): Boolean = {
    val propertiesToSet = mutatingPatterns.collect {
      case SetRelationshipPropertyPattern(_, key, _) => key
    }.toSet

    val propertiesToRead = qg.allKnownRelProperties.map(_.propertyKey)

    (propertiesToRead intersect propertiesToSet).nonEmpty
  }

  def deleteOverlap(qg: QueryGraph): Boolean = {
    val identifiersToRead = qg.patternNodes ++ qg.patternRelationships.map(_.name)
    (identifiersToRead intersect identifiersToDelete).nonEmpty
  }

  def addNodePatterns(nodePatterns: CreateNodePattern*): UpdateGraph =
    copy(mutatingPatterns = (this.mutatingPatterns ++ nodePatterns).distinct)

  def addRelPatterns(relationships: CreateRelationshipPattern*): UpdateGraph =
    copy(mutatingPatterns = (this.mutatingPatterns ++ relationships).distinct)

  def addSetLabel(setLabelPatterns: SetLabelPattern*): UpdateGraph =
    copy(mutatingPatterns = this.mutatingPatterns ++ setLabelPatterns)

  def addSetNodeProperty(setPropertyPatterns: SetNodePropertyPattern*): UpdateGraph =
    copy(mutatingPatterns = this.mutatingPatterns ++ setPropertyPatterns)

  def addSetRelProperty(setPropertyPatterns: SetRelationshipPropertyPattern*): UpdateGraph =
    copy(mutatingPatterns = this.mutatingPatterns ++ setPropertyPatterns)

  def addRemoveLabelPatterns(removeLabelPatterns: RemoveLabelPattern*): UpdateGraph =
    copy(mutatingPatterns = this.mutatingPatterns ++ removeLabelPatterns)

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

case class SetNodePropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends MutatingPattern

case class SetRelationshipPropertyPattern(idName: IdName, propertyKey: PropertyKeyName, expression: Expression) extends MutatingPattern

case class RemoveLabelPattern(idName: IdName, labels: Seq[LabelName]) extends MutatingPattern

case class DeleteExpression(expression: Expression, forced: Boolean) extends MutatingPattern

/*
 * Used to simplify finding overlap between writing and reading properties
 */
trait CreatesPropertyKeys {
  def overlaps(propertyKeyName: PropertyKeyName): Boolean
}

/*
 * CREATE (a:L)
 */
case object CreatesNoPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = false
}

/*
 * CREATE ({prop1: 42, prop2: 42})
 */
case class CreatesKnownPropertyKeys(keys: Set[PropertyKeyName]) extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName): Boolean = keys(propertyKeyName)
}

/*
 * CREATE ({props})
 */
case object CreatesUnknownPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = true
}
