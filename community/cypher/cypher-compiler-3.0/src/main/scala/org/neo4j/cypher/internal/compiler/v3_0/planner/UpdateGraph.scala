/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.frontend.v3_0.ast._

import scala.annotation.tailrec

case class UpdateGraph(mutatingPatterns: Seq[MutatingPattern] = Seq.empty) {

  def ++(other: UpdateGraph) = copy(mutatingPatterns = mutatingPatterns ++ other.mutatingPatterns)

  def isEmpty = this == UpdateGraph.empty

  def nonEmpty = !isEmpty

  /*
   * Finds all nodes being created with CREATE (a)
   */
  def createNodePatterns = mutatingPatterns.collect {
    case p: CreateNodePattern => p
  }


  def mergeNodePatterns = mutatingPatterns.collect {
    case m: MergeNodePattern => m
  }

  /*
   * Finds all nodes being created with CREATE ()-[r]->()
   */
  def createRelationshipPatterns: Seq[CreateRelationshipPattern] = mutatingPatterns.collect {
    case p: CreateRelationshipPattern => p
  }

  /*
   * Finds all identifiers being deleted.
   */
  def identifiersToDelete = (deleteExpressions flatMap {
    // DELETE n
    case DeleteExpression(identifier:Variable, _) => Seq(IdName.fromVariable(identifier))
    // DELETE (n)-[r]-()
    case DeleteExpression(PathExpression(e), _) => e.dependencies.map(IdName.fromVariable)
    // DELETE many[{i}]
    case DeleteExpression(ContainerIndex(identifier:Variable, _), _) => Seq(IdName.fromVariable(identifier))
  }).toSet

  /*
   * Finds all node properties being created with CREATE (:L)
   */
  def createLabels: Set[LabelName] = createNodePatterns.flatMap(_.labels).toSet ++ mergeNodePatterns.flatMap(_.createNodePattern.labels)

  /*
   * Finds all node properties being created with CREATE ({prop...})
   */
  def createNodeProperties = CreatesPropertyKeys(createNodePatterns.flatMap(_.properties):_*) +
    CreatesPropertyKeys(mergeNodePatterns.flatMap(_.createNodePattern.properties):_*)

  /*
   * Finds all rel properties being created with CREATE
   */
  def createRelProperties = CreatesPropertyKeys(createRelationshipPatterns.flatMap(_.properties):_*)

  /*
   * finds all label names being removed on given node, REMOVE a:L
   */
  def labelsToRemoveForNode(idName: IdName): Set[LabelName] = removeLabelPatterns.collect {
    case RemoveLabelPattern(n, labels) if n == idName => labels
  }.flatten.toSet

  /*
   * Relationship types being created with, CREATE ()-[:T]->()
   */
  def createRelTypes: Set[RelTypeName] = createRelationshipPatterns.map(_.relType).toSet

  /*
   * Does this UpdateGraph update nodes?
   */
  def updatesNodes: Boolean = createNodePatterns.nonEmpty || removeLabelPatterns.nonEmpty ||
    mergeNodePatterns.nonEmpty || setLabelPatterns.nonEmpty || setNodePropertyPatterns.nonEmpty

  /*
   * Does this UpdateGraph contains merges
   */
  def containsMerge: Boolean = mergeNodePatterns.nonEmpty

  /*
   * Checks if there is overlap between what's being read in the query graph
   * and what is being written here
   */
  def overlaps(qg: QueryGraph) =
      nonEmpty &&
      (createNodeOverlap(qg) || createRelationshipOverlap(qg) ||
        deleteOverlap(qg) || removeLabelOverlap(qg) || setLabelOverlap(qg) || setPropertyOverlap(qg)
        || mergeNodeDeleteOverlap)

  /*
   * Checks for overlap between nodes being read in the query graph
   * and those being created here
   */
  def createNodeOverlap(qg: QueryGraph) = {
    def labelsOverlap(labelsToRead: Set[LabelName], labelsToWrite: Set[LabelName]): Boolean = {
      labelsToRead.isEmpty || (labelsToRead intersect labelsToWrite).nonEmpty
    }
    def propsOverlap(propsToRead: Set[PropertyKeyName], propsToWrite: CreatesPropertyKeys) = {
      propsToRead.isEmpty || propsToRead.exists(propsToWrite.overlaps)
    }

    qg.patternNodes.exists(p => {
      val readProps = qg.allKnownPropertiesOnIdentifier(p).map(_.propertyKey)

      //MATCH () CREATE ()?
      qg.allKnownLabelsOnNode(p).isEmpty && readProps.isEmpty ||
        //MATCH (:B {prop:..}) CREATE (:B {prop:..})
        labelsOverlap(qg.allKnownLabelsOnNode(p).toSet, createLabels) &&
          propsOverlap(readProps, createNodeProperties)
    })
  }

  //if we do match delete and merge we always need to be eager
  def mergeNodeDeleteOverlap = deleteExpressions.nonEmpty && mergeNodePatterns.nonEmpty

  /*
   * Checks for overlap between rels being read in the query graph
   * and those being created here
   */
  def createRelationshipOverlap(qg: QueryGraph) = {
    def typesOverlap(typesToRead: Set[RelTypeName], typesToWrite: Set[RelTypeName]): Boolean = {
      typesToRead.isEmpty || (typesToRead intersect typesToWrite).nonEmpty
    }
    def propsOverlap(propsToRead: Set[PropertyKeyName], propsToWrite: CreatesPropertyKeys) = {
      propsToRead.isEmpty || propsToRead.exists(propsToWrite.overlaps)
    }

    //CREATE () MATCH ()-->()
    (createRelationshipPatterns.nonEmpty && qg.patternRelationships.nonEmpty) && qg.patternRelationships.exists(r => {
      val readProps = qg.allKnownPropertiesOnIdentifier(r.name).map(_.propertyKey)
      // CREATE ()-[]->() MATCH ()-[]-()?
      r.types.isEmpty && readProps.isEmpty ||
        // CREATE ()-[:T {prop:...}]->() MATCH ()-[:T {prop:{}]-()?
        (typesOverlap(r.types.toSet, createRelTypes) && propsOverlap(readProps, createRelProperties))
    })
  }

  /*
   * Checks for overlap between labels being read in query graph
   * and labels being updated with SET and MERGE here
   */
  def setLabelOverlap(qg: QueryGraph): Boolean = {

    @tailrec
    def toLabelPattern(patterns: Seq[MutatingPattern], acc: Seq[LabelName]): Seq[LabelName] = {

      def extractLabels(patterns: Seq[SetMutatingPattern]) = patterns.collect {
        case SetLabelPattern(_, labels) => labels
      }.flatten

      patterns match {
        case Nil => acc
        case SetLabelPattern(_, labels) :: tl => toLabelPattern(tl, acc ++ labels)
        case MergeNodePattern(_, _, onCreate, onMatch) :: tl =>
          toLabelPattern(tl, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
        case hd :: tl => toLabelPattern(tl, acc)
      }
    }

    val labelsToSet = toLabelPattern(mutatingPatterns, Seq.empty)
    qg.patternNodes.exists(p => qg.allKnownLabelsOnNode(p).intersect(labelsToSet).nonEmpty)
  }

  /*
   * Checks for overlap between what props are read in query graph
   * and what is updated with SET and MERGE here
   */
  def setPropertyOverlap(qg: QueryGraph) = setNodePropertyOverlap(qg) || setRelPropertyOverlap(qg)

  /*
   * Checks for overlap between identifiers being read in query graph
   * and what is deleted here
   */
  def deleteOverlap(qg: QueryGraph): Boolean = {
    val identifiersToRead = qg.patternNodes ++ qg.patternRelationships.map(_.name) ++ qg.argumentIds
    (identifiersToRead intersect identifiersToDelete).nonEmpty
  }

  def addMutatingPatterns(patterns: MutatingPattern *) =
    copy(mutatingPatterns = this.mutatingPatterns ++ patterns)

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

  /*
  * Checks for overlap between what node props are read in query graph
  * and what is updated with SET here
  */
  private def setNodePropertyOverlap(qg: QueryGraph): Boolean = {


    @tailrec
    def toNodePropertyPattern(patterns: Seq[MutatingPattern], acc: CreatesPropertyKeys): CreatesPropertyKeys = {

      def extractPropertyKey(patterns: Seq[SetMutatingPattern]): CreatesPropertyKeys = patterns.collect {
        case SetNodePropertyPattern(_, key, _) => CreatesKnownPropertyKeys(key)
        case SetNodePropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      }.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys)(_ + _)

      patterns match {
        case Nil => acc
        case SetNodePropertiesFromMapPattern(_, expression, _) :: tl => CreatesPropertyKeys(expression)
        case SetNodePropertyPattern(_, key, _) :: tl => toNodePropertyPattern(tl, acc + CreatesKnownPropertyKeys(key))
        case MergeNodePattern(_, _, onCreate, onMatch) :: tl =>
          toNodePropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case hd :: tl => toNodePropertyPattern(tl, acc)
      }
    }

    val propertiesToSet = toNodePropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)
    val propertiesToRead = qg.allKnownNodeProperties.map(_.propertyKey)

    propertiesToRead.exists(propertiesToSet.overlaps)
  }

  /*
   * Checks for overlap between what relationship props are read in query graph
   * and what is updated with SET her
   */
  private def setRelPropertyOverlap(qg: QueryGraph): Boolean = {
    val propertiesToSet = mutatingPatterns.collect {
      case SetRelationshipPropertyPattern(_, key, _) => key
    }.toSet

    val fromMapExpressions = mutatingPatterns.collect {
      case SetRelationshipPropertiesFromMapPattern(_, expression, _) => expression
    }
    val propertiesToSetFromMap = CreatesPropertyKeys(fromMapExpressions:_*)
    val propertiesToRead = qg.allKnownRelProperties.map(_.propertyKey)

    propertiesToRead.exists(propertiesToSetFromMap.overlaps) ||
      (propertiesToRead intersect propertiesToSet).nonEmpty
  }

  private def deleteExpressions = mutatingPatterns.collect {
    case p: DeleteExpression => p
  }

  private def removeLabelPatterns = mutatingPatterns.collect {
    case p: RemoveLabelPattern => p
  }

  private def setLabelPatterns = mutatingPatterns.collect {
    case p: SetLabelPattern => p
  }

  private def setNodePropertyPatterns = mutatingPatterns.collect {
    case p: SetNodePropertyPattern => p
    case p: SetNodePropertiesFromMapPattern => p
  }

  private def setRelationshipPropertyPatterns = mutatingPatterns.collect {
    case p: SetRelationshipPropertyPattern => p
    case p: SetRelationshipPropertiesFromMapPattern => p
  }
}

object UpdateGraph {
  val empty = UpdateGraph()
}
