/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.functions.Labels
import org.neo4j.cypher.internal.frontend.v3_4.symbols.TypeSpec
import org.neo4j.cypher.internal.frontend.v3_4.{InternalException, SemanticTable, symbols}

import scala.annotation.tailrec

trait UpdateGraph {

  def mutatingPatterns: Seq[MutatingPattern]

  def readOnly: Boolean = mutatingPatterns.isEmpty

  def containsUpdates: Boolean = !readOnly

  def containsMergeRecursive: Boolean = mergeNodePatterns.nonEmpty || mergeRelationshipPatterns.nonEmpty ||
    foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(_.containsMergeRecursive))

  /*
   * Finds all nodes being created with CREATE (a)
   */
  def createNodePatterns: Seq[CreateNodePattern] = mutatingPatterns.collect {
    case p: CreateNodePattern => p
  }

  def mergeNodePatterns: Seq[MergeNodePattern] = mutatingPatterns.collect {
    case m: MergeNodePattern => m
  }

  def mergeRelationshipPatterns: Seq[MergeRelationshipPattern] = mutatingPatterns.collect {
    case m: MergeRelationshipPattern => m
  }

  def foreachPatterns: Seq[ForeachPattern] = mutatingPatterns.collect {
    case p: ForeachPattern => p
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
  def identifiersToDelete: Set[IdName] = (deleteExpressions flatMap {
    // DELETE n
    case DeleteExpression(identifier: Variable, _) => Seq(IdName.fromVariable(identifier))
    // DELETE (n)-[r]-()
    case DeleteExpression(PathExpression(e), _) => e.dependencies.map(IdName.fromVariable)
    // DELETE expr
    case DeleteExpression(expr, _) => Seq(findVariableInNestedStructure(expr))
  }).toSet

  @tailrec
  private def findVariableInNestedStructure(e: Expression): IdName = e match {
    case v: Variable => IdName.fromVariable(v)
    // DELETE coll[i]
    case ContainerIndex(expr, _) => findVariableInNestedStructure(expr)
    // DELETE map.key
    case Property(expr, _) => findVariableInNestedStructure(expr)
  }

  /*
   * Finds all node properties being created with CREATE (:L)
   */
  def createLabels: Set[LabelName] = createNodePatterns.flatMap(_.labels).toSet ++
    mergeNodePatterns.flatMap(_.createNodePattern.labels) ++
    mergeRelationshipPatterns.flatMap(_.createNodePatterns.flatMap(_.labels))

  /*
   * Finds all node properties being created with CREATE ({prop...})
   */
  def createNodeProperties: CreatesPropertyKeys = CreatesPropertyKeys(createNodePatterns.flatMap(_.properties):_*) +
    CreatesPropertyKeys(mergeNodePatterns.flatMap(_.createNodePattern.properties):_*) +
    CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createNodePatterns.flatMap(c => c.properties)):_*)

  /*
   * Finds all rel properties being created with CREATE
   */
  def createRelProperties: CreatesPropertyKeys = CreatesPropertyKeys(createRelationshipPatterns.flatMap(_.properties):_*) +
    CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createRelPatterns.flatMap(c => c.properties)):_*)

  /*
   * finds all label names being removed on given node, REMOVE a:L
   */
  def labelsToRemoveFromOtherNodes(idName: IdName): Set[LabelName] = removeLabelPatterns.collect {
    case RemoveLabelPattern(n, labels) if n != idName => labels
  }.flatten.toSet

  /*
   * Relationship types being created with, CREATE/MERGE ()-[:T]->()
   */
  def createRelTypes: Set[RelTypeName] = (createRelationshipPatterns.map(_.relType) ++
    mergeRelationshipPatterns.flatMap(_.createRelPatterns.map(_.relType))).toSet

  /*
   * Does this UpdateGraph update nodes?
   */
  // NOTE: Put foreachPatterns first to shortcut unnecessary recursion
  def updatesNodes: Boolean = foreachPatterns.nonEmpty || createNodePatterns.nonEmpty || removeLabelPatterns.nonEmpty ||
    mergeNodePatterns.nonEmpty || mergeRelationshipPatterns.nonEmpty || setLabelPatterns.nonEmpty ||
    setNodePropertyPatterns.nonEmpty

  def foreachOverlap(qg: QueryGraph): Boolean =
    this != qg && // Foreach does not overlap itself
      // Conservatively always assume overlap for now
      (this.foreachPatterns.nonEmpty && qg.containsReads ||
        qg.foreachPatterns.nonEmpty && qg.containsMergeRecursive && this.containsUpdates)
    // TODO: We can be more precise and recursively check for overlaps inside nested foreach instead, e.g.
    // (foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(ug => ug.overlaps(qg) /* Read-Write */ ||
    //  qg.foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(x => ug.overlaps(x))) /* Write-Read */)))
    //  ...

  /*
   * Checks if there is overlap between what is being read in the query graph
   * and what is being written here
   */
  def overlaps(qg: QueryGraph): Boolean = {
    containsUpdates && {
      val readQg = qg.mergeQueryGraph.getOrElse(qg)

      createNodeOverlap(readQg) || createRelationshipOverlap(readQg) || deleteOverlap(readQg) ||
        removeLabelOverlap(readQg) || setLabelOverlap(readQg) || setPropertyOverlap(readQg) ||
        deleteOverlapWithMergeIn(qg) || foreachOverlap(readQg)
    }
  }

  /*
   * Determines whether there's an overlap in writes being done here, and reads being done in the given horizon.
   */
  def overlapsHorizon(horizon: QueryHorizon, semanticTable: SemanticTable): Boolean =
    containsUpdates && ({
      val propertiesReadInHorizon = horizon.dependingExpressions.collect {
        case p: Property => p
      }.toSet

      val maybeNode: Property => Boolean = maybeType(semanticTable, symbols.CTNode.invariant)
      val maybeRel: Property => Boolean = maybeType(semanticTable, symbols.CTRelationship.invariant)

      setNodePropertyOverlap(propertiesReadInHorizon.filter(maybeNode).map(_.propertyKey)) ||
      setRelPropertyOverlap(propertiesReadInHorizon.filter(maybeRel).map(_.propertyKey))
    } || ((labelsToSet.nonEmpty || removeLabelPatterns.nonEmpty) && usesLabelsFunction(horizon)))

  def writeOnlyHeadOverlaps(qg: QueryGraph): Boolean = {
    containsUpdates && {
      val readQg = qg.mergeQueryGraph.getOrElse(qg)

      deleteOverlap(readQg) ||
        deleteOverlapWithMergeIn(qg)
    }
  }

  def createsNodes: Boolean = mutatingPatterns.exists {
    case _: CreateNodePattern => true
    case _: MergeNodePattern => true
    case MergeRelationshipPattern(nodesToCreate, _, _, _, _) => nodesToCreate.nonEmpty
    case _ => false
  }

  /*
   * Checks for overlap between nodes being read in the query graph
   * and those being created here
   */
  def createNodeOverlap(qg: QueryGraph): Boolean = {
    def labelsOverlap(labelsToRead: Set[LabelName], labelsToWrite: Set[LabelName]): Boolean = {
      labelsToRead.isEmpty || (labelsToRead intersect labelsToWrite).nonEmpty
    }
    def propsOverlap(propsToRead: Set[PropertyKeyName], propsToWrite: CreatesPropertyKeys) = {
      propsToRead.isEmpty || propsToRead.exists(propsToWrite.overlaps)
    }

    val nodesRead: Set[IdName] = qg.allPatternNodesRead.filterNot(qg.argumentIds)

    createsNodes && nodesRead.exists(p => {
      val readProps = qg.allKnownPropertiesOnIdentifier(p).map(_.propertyKey)

      //MATCH () CREATE ()?
      qg.allKnownLabelsOnNode(p).isEmpty && readProps.isEmpty ||
        //MATCH (:B {prop:..}) CREATE (:B {prop:..})
        labelsOverlap(qg.allKnownLabelsOnNode(p), createLabels) &&
          propsOverlap(readProps, createNodeProperties)
    })
  }

  //if we do match delete and merge we always need to be eager
  def deleteOverlapWithMergeIn(other: UpdateGraph): Boolean =
    deleteExpressions.nonEmpty && (other.mergeNodePatterns.nonEmpty || other.mergeRelationshipPatterns.nonEmpty)
    // NOTE: As long as we have the conservative eagerness rule for FOREACH we do not need this recursive check
    // || other.foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(deleteOverlapWithMergeIn)))

  /*
   * Checks for overlap between rels being read in the query graph
   * and those being created here
   */
  def createRelationshipOverlap(qg: QueryGraph): Boolean = {
    def typesOverlap(typesToRead: Set[RelTypeName], typesToWrite: Set[RelTypeName]): Boolean = {
      typesToRead.isEmpty || (typesToRead intersect typesToWrite).nonEmpty
    }
    def propsOverlap(propsToRead: Set[PropertyKeyName], propsToWrite: CreatesPropertyKeys) = {
      propsToRead.isEmpty || propsToRead.exists(propsToWrite.overlaps)
    }

    val allRelPatternsWritten = createRelationshipPatterns ++ mergeRelationshipPatterns.flatMap(_.createRelPatterns)
    val allRelPatternsRead = qg.allPatternRelationshipsRead

    //CREATE () MATCH ()-->()
    (allRelPatternsWritten.nonEmpty && qg.patternRelationships.nonEmpty) && allRelPatternsRead.exists(r => {
      val readProps = qg.allKnownPropertiesOnIdentifier(r.name).map(_.propertyKey)
      // CREATE ()-[]->() MATCH ()-[]-()?
      r.types.isEmpty && readProps.isEmpty ||
        // CREATE ()-[:T {prop:...}]->() MATCH ()-[:T {prop:{}]-()?
        (typesOverlap(r.types.toSet, createRelTypes) && propsOverlap(readProps, createRelProperties))
    })
  }


  def labelsToSet: Set[LabelName] = {
    @tailrec
    def toLabelPattern(patterns: Seq[MutatingPattern], acc: Set[LabelName]): Set[LabelName] = {

      def extractLabels(patterns: Seq[SetMutatingPattern]) = patterns.collect {
        case SetLabelPattern(_, labels) => labels
      }.flatten

      patterns match {
        case Nil => acc
        case SetLabelPattern(_, labels) :: tl => toLabelPattern(tl, acc ++ labels)
        case MergeNodePattern(_, _, onCreate, onMatch) :: tl =>
          toLabelPattern(tl, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) :: tl =>
          toLabelPattern(tl, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
        case hd :: tl => toLabelPattern(tl, acc)
      }
    }

    toLabelPattern(mutatingPatterns, Set.empty)
  }

  /*
   * Checks for overlap between labels being read in query graph
   * and labels being updated with SET and MERGE here
   */
  def setLabelOverlap(qg: QueryGraph): Boolean =
    qg.patternNodes.filterNot(qg.argumentIds)
      .exists(p => qg.allKnownLabelsOnNode(p).intersect(labelsToSet).nonEmpty)

  /*
   * Checks for overlap between what props are read in query graph
   * and what is updated with SET and MERGE here
   */
  def setPropertyOverlap(qg: QueryGraph): Boolean =
    setNodePropertyOverlap(qg.allKnownNodeProperties.map(_.propertyKey)) ||
      setRelPropertyOverlap(qg.allKnownRelProperties.map(_.propertyKey))

  /*
   * Checks for overlap between identifiers being read in query graph
   * and what is deleted here
   */
  def deleteOverlap(qg: QueryGraph): Boolean = {
    // TODO:H FIXME qg.argumentIds here is not correct, but there is a unit test that depends on it
    val identifiersToRead = qg.allPatternNodesRead ++ qg.allPatternRelationshipsRead.map(_.name) ++ qg.argumentIds
    (identifiersToRead intersect identifiersToDelete).nonEmpty
  }

  private def usesLabelsFunction(horizon: QueryHorizon) = {
    horizon.dependingExpressions.exists {
      case f: FunctionInvocation => f.function == Labels
      case _ => false
    }
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

  /**
   * Checks whether the expression that a property is called on could be of type `typeSpec`.
   */
  def maybeType(semanticTable: SemanticTable, typeSpec: TypeSpec)(p:Property): Boolean =
    semanticTable.types.get(p.map) match {
      case Some(expressionTypeInfo) =>
        val actualType = expressionTypeInfo.actual
        actualType == typeSpec || actualType == symbols.CTAny.invariant

      case None => throw new InternalException(s"Expression ${p.map} has to type from semantic analysis")
    }

  /*
  * Checks for overlap between what node props are read in query graph
  * and what is updated with SET here (properties added by create/merge directly is handled elsewhere)
  */
  private def setNodePropertyOverlap(propertiesToRead: Set[PropertyKeyName]): Boolean = {

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
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) :: tl =>
          toNodePropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case hd :: tl => toNodePropertyPattern(tl, acc)
      }
    }

    val propertiesToSet = toNodePropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)

    propertiesToRead.exists(propertiesToSet.overlaps)
  }

  /*
   * Checks for overlap between what relationship props are read in query graph
   * and what is updated with SET her
   */
  private def setRelPropertyOverlap(propertiesToRead: Set[PropertyKeyName]): Boolean = {
    @tailrec
    def toRelPropertyPattern(patterns: Seq[MutatingPattern], acc: CreatesPropertyKeys): CreatesPropertyKeys = {

      def extractPropertyKey(patterns: Seq[SetMutatingPattern]): CreatesPropertyKeys = patterns.collect {
        case SetRelationshipPropertyPattern(_, key, _) => CreatesKnownPropertyKeys(key)
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      }.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys)(_ + _)

      patterns match {
        case Nil => acc
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) :: tl => CreatesPropertyKeys(expression)
        case SetRelationshipPropertyPattern(_, key, _) :: tl =>
          toRelPropertyPattern(tl, acc + CreatesKnownPropertyKeys(key))
        case MergeNodePattern(_, _, onCreate, onMatch) :: tl =>
          toRelPropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) :: tl =>
          toRelPropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case hd :: tl => toRelPropertyPattern(tl, acc)
      }
    }

    val propertiesToSet = toRelPropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)

    propertiesToRead.exists(propertiesToSet.overlaps)
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

  def mergeQueryGraph: Option[QueryGraph] = mutatingPatterns.collect {
    case c: MergePattern => c.matchGraph
  }.headOption
}
