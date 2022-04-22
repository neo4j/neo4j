/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.UnstableIdentifier
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

import scala.annotation.tailrec

trait UpdateGraph {

  def mutatingPatterns: Seq[MutatingPattern]

  def readOnly: Boolean = mutatingPatterns.isEmpty

  def containsUpdates: Boolean = !readOnly

  def containsMergeRecursive: Boolean = hasMergeNodePatterns || hasMergeRelationshipPatterns ||
    foreachPatterns.exists(_.innerUpdates.allQGsWithLeafInfo.map(_.queryGraph).exists(_.containsMergeRecursive))

  def containsPropertyReadsInUpdates: Boolean = mutatingPatterns.folder.treeExists {
    case _:Property => true
    case _:ContainerIndex => true
  }

  /*
   * Finds all nodes being created with CREATE ...
   */
  def createPatterns: Seq[CreatePattern] = mutatingPatterns.collect {
    case p: CreatePattern => p
  }

  def hasCreatePatterns: Boolean = mutatingPatterns.exists {
    case _: CreatePattern => true
    case _ => false
  }

  def mergeNodePatterns: Seq[MergeNodePattern] = mutatingPatterns.collect {
    case m: MergeNodePattern => m
  }

  def hasMergeNodePatterns: Boolean = mutatingPatterns.exists {
    case _: MergeNodePattern => true
    case _ => false
  }

  def mergeRelationshipPatterns: Seq[MergeRelationshipPattern] = mutatingPatterns.collect {
    case m: MergeRelationshipPattern => m
  }

  def hasMergeRelationshipPatterns: Boolean = mutatingPatterns.exists {
    case _: MergeRelationshipPattern => true
    case _ => false
  }

  def foreachPatterns: Seq[ForeachPattern] = mutatingPatterns.collect {
    case p: ForeachPattern => p
  }

  def hasForeachPatterns: Boolean = mutatingPatterns.exists {
    case _: ForeachPattern => true
    case _ => false
  }

  /*
   * Finds all identifiers being deleted.
   */
  def identifiersToDelete: Set[String] = (deleteExpressions flatMap {
    // DELETE n
    // DELETE (n)-[r]-()
    // DELETE expr
    case DeleteExpression(expr, _) => expr.dependencies.map(_.name)
  }).toSet

  /*
   * Finds all node properties being created with CREATE (:L)
   */
  lazy val createLabels: Set[Set[LabelName]] =
    createPatterns.flatMap(_.nodes).map(_.labels.toSet).toSet ++
    mergeNodePatterns.map(_.createNode.labels.toSet) ++
    mergeRelationshipPatterns.flatMap(_.createNodes).map(_.labels.toSet)

  /*
   * Finds all node properties being created with CREATE ({prop...})
   */
  lazy val createNodeProperties: CreatesPropertyKeys =
    CreatesPropertyKeys(createPatterns.flatMap(_.nodes.flatMap(_.properties)):_*) +
    CreatesPropertyKeys(mergeNodePatterns.flatMap(_.createNode.properties):_*) +
    CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createNodes.flatMap(c => c.properties)):_*)

  /*
   * Finds all rel properties being created with CREATE
   */
  lazy val createRelProperties: CreatesPropertyKeys =
    CreatesPropertyKeys(createPatterns.flatMap(_.relationships.flatMap(_.properties)):_*) +
    CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createRelationships.flatMap(c => c.properties)):_*)

  /*
   * finds all label names being removed on given node, REMOVE a:L
   */
  def labelsToRemoveFromOtherNodes(idName: String): Set[LabelName] = removeLabelPatterns.collect {
    case RemoveLabelPattern(n, labels) if n != idName => labels
  }.flatten.toSet

  /*
   * Relationship types being created with, CREATE/MERGE ()-[:T]->()
   */
  lazy val createRelTypes: Set[RelTypeName] =
    (createPatterns.flatMap(_.relationships.map(_.relType)) ++
     mergeRelationshipPatterns.flatMap(_.createRelationships.map(_.relType))).toSet

  /*
   * Does this UpdateGraph update nodes?
   */
  // NOTE: Put foreachPatterns first to shortcut unnecessary recursion
  lazy val updatesNodes: Boolean =
    hasForeachPatterns ||
    createPatterns.exists(_.nodes.nonEmpty) ||
    hasRemoveLabelPatterns ||
    hasMergeNodePatterns ||
    hasMergeRelationshipPatterns ||
    hasSetLabelPatterns ||
    hasSetNodePropertyPatterns

  // TODO: We can be more precise and recursively check for overlaps inside nested foreach instead, e.g.
  //  (foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(ug => ug.overlaps(qg) /* Read-Write */ ||
  //  qg.foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(x => ug.overlaps(x))) /* Write-Read */)))
  //  ...
  def foreachOverlap(qgWithInfo: QgWithLeafInfo): Boolean = {
    val qg = qgWithInfo.queryGraph
    qgWithInfo.hasUnstableLeaves &&
      this != qg && // Foreach does not overlap itself
      (this.hasForeachPatterns && qg.containsReads || // Conservatively always assume overlap for now
        qg.hasForeachPatterns && qg.containsMergeRecursive && this.containsUpdates)
  }

  /*
   * Checks if there is overlap between what is being read in the query graph
   * and what is being written here
   */
  def overlaps(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Seq[EagernessReason.Reason] = {
    if (!containsUpdates) {
      Seq.empty
    } else {
      // A MERGE is always on its own in a QG. That's why we pick either the read graph of a MERGE or the qg itself.
      val readQg = qgWithInfo.queryGraph.mergeQueryGraph.map(mergeQg => qgWithInfo.copy(solvedQg = mergeQg)).getOrElse(qgWithInfo)

      lazy val unknownReasons = nodeOverlap(readQg) ||
        createRelationshipOverlap(readQg) ||
        setPropertyOverlap(readQg) ||
        deleteOverlapWithMergeIn(qgWithInfo.queryGraph) ||
        foreachOverlap(readQg)

      val checkers = Seq(
        // TODO: Update to use label expression overlap
        deleteOverlap(_),
        removeLabelOverlap(_),
        setLabelOverlap(_),
      )

      val reasons = checkers.map(c => c(readQg)).collect{case Some(reason) => reason}

      if (reasons.nonEmpty) {
        reasons
      } else if (unknownReasons) {
        Seq(EagernessReason.Unknown)
      } else {
        Seq.empty
      }
    }
  }

  /*
   * Determines whether there's an overlap in writes being done here, and reads being done in the given horizon.
   */
  def overlapsHorizon(horizon: QueryHorizon)(implicit semanticTable: SemanticTable): Seq[EagernessReason.Reason] = {
    if(!containsUpdates || !horizon.couldContainRead)
      Seq.empty
    else {
      horizon.allQueryGraphs.flatMap(overlaps)
    }
  }

  def writeOnlyHeadOverlaps(qgWithInfo: QgWithLeafInfo): Option[EagernessReason.Reason] = {
    if (!containsUpdates) {
      None
    } else {
      val readQg = qgWithInfo.queryGraph.mergeQueryGraph.map(mergeQg => qgWithInfo.copy(solvedQg = mergeQg)).getOrElse(qgWithInfo)

      val overlap = deleteOverlap(readQg)

      if (overlap.nonEmpty) {
        overlap
      } else if (deleteOverlapWithMergeIn(qgWithInfo.queryGraph)) {
        Some(EagernessReason.Unknown)
      } else {
        None
      }
    }
  }

  def createsNodes: Boolean = mutatingPatterns.exists {
    case c: CreatePattern if c.nodes.nonEmpty => true
    case _: MergeNodePattern => true
    case MergeRelationshipPattern(nodesToCreate, _, _, _, _) => nodesToCreate.nonEmpty
    case _ => false
  }

  /*
   * Check if the labels or properties of any unstable leaf node overlaps
   * with the labels or properties updated in this query. This may cause the read to affected
   * by the writes.
   */
  def nodeOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Boolean = {
    val labelsToCreate = createLabels
    val propertiesToCreate = createNodeProperties
    val tailCreatesNodes = createsNodes

    val relevantNodes = qgWithInfo.nonArgumentPatternNodes(semanticTable) intersect qgWithInfo.leafPatternNodes
    updatesNodes && relevantNodes.exists { currentNode =>
      currentNode match {
        case _:StableIdentifier => false
        case _:UnstableIdentifier =>
          val labelsOnCurrentNode = qgWithInfo.allKnownUnstableNodeLabelsFor(currentNode)
          val propertiesOnCurrentNode = qgWithInfo.allKnownUnstablePropertiesFor(currentNode)
          lazy val labelsToRemove = labelsToRemoveFromOtherNodes(currentNode.name)

          val noLabelOrPropOverlap = labelsOnCurrentNode.isEmpty && propertiesOnCurrentNode.isEmpty && tailCreatesNodes

          //MATCH () CREATE/MERGE (...)?
          noLabelOrPropOverlap ||
            //MATCH (A&B|!C) CREATE (:A:B)
            (labelsOnCurrentNode.nonEmpty && labelExpressionsOverlap(qgWithInfo, labelsToCreate, Set(currentNode.name))) || //MATCH ({prop:42}) CREATE ({prop:...})
            (labelsOnCurrentNode.isEmpty &&
            propertiesOnCurrentNode.exists(propertiesToCreate.overlaps)) ||
          //MATCH (n:A), (m:B) REMOVE n:B
          //MATCH (n:A), (m:A) REMOVE m:A
          (labelsToRemove intersect labelsOnCurrentNode).nonEmpty}
    }
  }

  /*
   * Uses an expression evaluator to figure out if we have a label overlap.
   * For example, if we have `CREATE (:A:B)` we need to solve the predicates given labels A and B (and no other labels).
   * For predicates which contains non label expressions we default to true.
   *
   * If we have multiple predicates, we will only have an overlap if all predicates are evaluated to true.
   * For example, if we have `MATCH (n) WHERE n:A AND n:B CREATE (:A)` we don't need to insert an eager since the predicate `(n:B)` will be evaluated to false.
   */
  private def labelExpressionsOverlap(qgWithInfo: QgWithLeafInfo, labelsToCreate: Set[Set[LabelName]], currentNode: Set[String]): Boolean = {
    val predicates = qgWithInfo.queryGraph.selections.predicates.map(_.expr) ++
      qgWithInfo.queryGraph.optionalMatches.map(_.selections).flatMap(_.predicates).map(_.expr)

    predicates.forall { expression =>
      labelsToCreate.exists(labelToCreate => labelExpressionOverlaps(currentNode, expression, labelToCreate.map(_.name)).getOrElse(true))
    }
  }

  private def labelExpressionOverlaps(currentNode: Set[String], expression: Expression, createdLabels: Set[String]): Option[Boolean] = {
    expression match {
      case HasLabels(Variable(node), labels) if currentNode.contains(node) => Some(createdLabels.exists(labels.map(_.name).contains))
      case And(lhs, rhs) => evalBinFunc(currentNode, lhs, rhs, createdLabels, (lhs, rhs) => lhs && rhs)
      case Or(lhs, rhs) => evalBinFunc(currentNode, lhs, rhs, createdLabels, (lhs, rhs) => lhs || rhs)
      case Not(expr) => labelExpressionOverlaps(currentNode, expr, createdLabels).map(!_)
      case Ors(exprs) =>
        val evaluatedExprs = exprs.map(expr => labelExpressionOverlaps(currentNode, expr, createdLabels))
        if (evaluatedExprs.contains(None)) {
          None
        } else {
         Some(evaluatedExprs.flatten.contains(true))
        }
      case Ands(exprs) =>
        val evaluatedExprs = exprs.map(expr => labelExpressionOverlaps(currentNode, expr, createdLabels))
        if (evaluatedExprs.contains(None)) {
          None
        } else {
          Some(!evaluatedExprs.flatten.contains(false))
        }
      case _ => None
    }
  }

  private def evalBinFunc(currentNode: Set[String], a: Expression, b: Expression, labels: Set[String], op: (Boolean, Boolean) => Boolean): Option[Boolean] =
    labelExpressionOverlaps(currentNode, a, labels)
      .flatMap(lhs =>
        labelExpressionOverlaps(currentNode, b, labels)
          .map(rhs => op(lhs, rhs))
      )

  //if we do match delete and merge we always need to be eager
  def deleteOverlapWithMergeIn(other: UpdateGraph): Boolean =
    hasDeleteExpressions && (other.hasMergeNodePatterns || other.hasMergeRelationshipPatterns)
    // NOTE: As long as we have the conservative eagerness rule for FOREACH we do not need this recursive check
    // || other.foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(deleteOverlapWithMergeIn)))

  /*
   * Checks for overlap between rels being read in the query graph
   * and those being created here
   */
  def createRelationshipOverlap(qgWithInfo: QgWithLeafInfo): Boolean = {
    // MATCH ()-->() CREATE ()-->()
    allRelPatternsWrittenNonEmpty && qgWithInfo.patternRelationships.exists(r => {
      if (r.isIdStable) {
        false
      } else {
        val readProps = qgWithInfo.allKnownUnstablePropertiesFor(r)
        val types = qgWithInfo.allPossibleUnstableRelTypesFor(r)
        relationshipOverlap(types, readProps)
      }
    })
  }

  def createRelationshipOverlapHorizon(allRelPatternsRead: Set[RelationshipPattern]): Boolean = {
    //CREATE () MATCH ()-->()
    (allRelPatternsWrittenNonEmpty && allRelPatternsRead.nonEmpty) && allRelPatternsRead.exists(r => {
      r.properties match {
        case Some(MapExpression(items)) =>
          val propKeyNames = items.map(_._1).toSet
          relationshipOverlap(r.types.toSet, propKeyNames)
        case _ => false
      }
    })
  }

  lazy val allRelPatternsWrittenNonEmpty: Boolean = {
    val allRelPatternsWritten =
      createPatterns.filter(_.relationships.nonEmpty) ++ mergeRelationshipPatterns.flatMap(_.createRelationships)

    allRelPatternsWritten.nonEmpty
  }

  private def relationshipOverlap(readRelTypes: Set[RelTypeName], readRelProperties: Set[PropertyKeyName]): Boolean = {
    def typesOverlap(typesToRead: Set[RelTypeName], typesToWrite: Set[RelTypeName]): Boolean = {
      typesToRead.isEmpty || (typesToRead intersect typesToWrite).nonEmpty
    }
    def propsOverlap(propsToRead: Set[PropertyKeyName], propsToWrite: CreatesPropertyKeys) = {
      propsToRead.isEmpty || propsToRead.exists(propsToWrite.overlaps)
    }
    // CREATE ()-[]->() MATCH ()-[]-()?
    readRelTypes.isEmpty && readRelProperties.isEmpty ||
      // CREATE ()-[:T {prop:...}]->() MATCH ()-[:T {prop:{}]-()?
      (typesOverlap(readRelTypes, createRelTypes) && propsOverlap(readRelProperties, createRelProperties))
  }


  lazy val labelsToSet: Set[LabelName] = {
    @tailrec
    def toLabelPattern(patterns: Seq[MutatingPattern], acc: Set[LabelName]): Set[LabelName] = {

      def extractLabels(patterns: Seq[SetMutatingPattern]) = patterns.collect {
        case SetLabelPattern(_, labels) => labels
      }.flatten

      if (patterns.isEmpty) acc
      else patterns.head match {
        case SetLabelPattern(_, labels) => toLabelPattern(patterns.tail, acc ++ labels)
        case MergeNodePattern(_, _, onCreate, onMatch) =>
          toLabelPattern(patterns.tail, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
          toLabelPattern(patterns.tail, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
        case _ => toLabelPattern(patterns.tail, acc)
      }
    }

    toLabelPattern(mutatingPatterns, Set.empty)
  }

  /*
   * Checks for overlap between labels being read in query graph
   * and labels being updated with SET and MERGE here
   */
  def setLabelOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Option[EagernessReason.Reason] = {
    // For SET label, we even have to look at the arguments for which we don't know if they are a node or not, so we consider HasLabelsOrTypes predicates.
    def overlapWithKnownLabels = qgWithInfo.patternNodesAndArguments(semanticTable)
      .flatMap(p => qgWithInfo.allKnownUnstableNodeLabelsFor(p).intersect(labelsToSet))
    def overlapWithLabelsFunction = qgWithInfo.folder.treeExists {
      case f: FunctionInvocation => f.function == Labels
    }

    if (overlapWithKnownLabels.nonEmpty)
      Some(EagernessReason.OverlappingSetLabels(overlapWithKnownLabels.toSeq.map(_.name)))
    else if (overlapWithLabelsFunction)
      Some(EagernessReason.OverlappingSetLabels(Seq.empty))
    else
      None
  }

  /*
   * Checks for overlap between what props are read in query graph
   * and what is updated with SET and MERGE here
   */
  def setPropertyOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Boolean = {
    val hasDynamicProperties = qgWithInfo.folder.treeExists {
      case ContainerIndex(_, _: Literal) => false
      case _: ContainerIndex => true
    }

    val readPropKeys = getReadPropKeys(qgWithInfo)

    setNodePropertyOverlap(readPropKeys.nodePropertyKeys, hasDynamicProperties) ||
      setRelPropertyOverlap(readPropKeys.relPropertyKeys, hasDynamicProperties)
  }

  private case class ReadPropKeys(nodePropertyKeys: Set[PropertyKeyName], relPropertyKeys: Set[PropertyKeyName])

  private def getReadPropKeys(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): ReadPropKeys = {
    val (readNodePropKeys, readRelPropKeys, readOtherPropKeys) =
      // Don't do this when comparing against self, to avoid finding overlap for e.g. SET n.prop = n.prop + 1
      if (this != qgWithInfo.queryGraph) {
        val readProps = qgWithInfo.queryGraph.mutatingPatterns.folder.findAllByClass[Property]
        val (readNodeProps, readRelOrOtherProps) = readProps.partition(p => semanticTable.isNodeNoFail(p.map))
        val (readRelProps, readOtherProps) = readRelOrOtherProps.partition(p => semanticTable.isRelationshipNoFail(p.map))

        (readNodeProps.map(_.propertyKey), readRelProps.map(_.propertyKey), readOtherProps.map(_.propertyKey))
      } else {
        (Set.empty, Set.empty, Set.empty)
      }


    ReadPropKeys(
      qgWithInfo.allKnownUnstableNodeProperties(semanticTable) ++ readNodePropKeys ++ readOtherPropKeys,
      qgWithInfo.allKnownUnstableRelProperties(semanticTable) ++ readRelPropKeys ++ readOtherPropKeys
    )
  }

  /*
   * Checks for overlap between identifiers being read in query graph
   * and what is deleted here
   */
  def deleteOverlap(qgWithInfo: QgWithLeafInfo): Option[EagernessReason.Reason] = {
    // TODO:H FIXME qg.argumentIds here is not correct, but there is a unit test that depends on it
    val identifiersToRead = qgWithInfo.unstablePatternNodes ++ qgWithInfo.queryGraph.allPatternRelationshipsRead.map(_.name) ++ qgWithInfo.queryGraph.argumentIds
    val overlaps = (identifiersToRead intersect identifiersToDelete).toSeq
    if (overlaps.nonEmpty) {
      Some(EagernessReason.DeleteOverlap(overlaps))
    } else {
      None
    }
  }

  def removeLabelOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Option[EagernessReason.Reason] = {
    val otherLabelsRead = qgWithInfo.allKnownUnstableNodeLabels(semanticTable)

    val overlappingLabels: Seq[LabelName] = removeLabelPatterns.collect {
      case RemoveLabelPattern(_, labelsToRemove) =>
        //does any other identifier match on the labels I am deleting?
        //MATCH (a:BAR)..(b) REMOVE b:BAR
        labelsToRemove.filter(l => {
          otherLabelsRead(l)
        })
    }.flatten

    if (overlappingLabels.nonEmpty) {
      Some(EagernessReason.OverlappingDeletedLabels(overlappingLabels.map(_.name)))
    } else {
      None
    }
  }

  /*
  * Checks for overlap between what node props are read in query graph
  * and what is updated with SET here (properties added by create/merge directly is handled elsewhere)
  */
  private def setNodePropertyOverlap(propertiesToRead: Set[PropertyKeyName], hasDynamicProperties: Boolean): Boolean = {

    @tailrec
    def toNodePropertyPattern(patterns: Seq[MutatingPattern], acc: CreatesPropertyKeys): CreatesPropertyKeys = {

      def extractPropertyKey(patterns: Seq[SetMutatingPattern]): CreatesPropertyKeys = patterns.collect {
        case SetNodePropertyPattern(_, key, _) => CreatesKnownPropertyKeys(key)
        case SetNodePropertiesPattern(_, items) => CreatesKnownPropertyKeys(items.map(_._1).toSet)
        case SetNodePropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
        case SetPropertiesPattern(_, items) => CreatesPropertyKeys(items.map(_._2):_*)
        case SetPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      }.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys)(_ + _)

      if (patterns.isEmpty) acc
      else patterns.head match {
        case SetNodePropertiesFromMapPattern(_, expression, _)  => CreatesPropertyKeys(expression)
        case SetPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
        case SetNodePropertyPattern(_, key, _)  => toNodePropertyPattern(patterns.tail, acc + CreatesKnownPropertyKeys(key))
        case SetNodePropertiesPattern(_, items)  => toNodePropertyPattern(patterns.tail, acc + CreatesPropertyKeys(items.map(_._2):_*))
        case SetPropertiesPattern(_, items)  => toNodePropertyPattern(patterns.tail, acc + CreatesKnownPropertyKeys(items.map(_._1).toSet))
        case MergeNodePattern(_, _, onCreate, onMatch) =>
          toNodePropertyPattern(patterns.tail, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
          toNodePropertyPattern(patterns.tail, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case _ => toNodePropertyPattern(patterns.tail, acc)
      }
    }

    val propertiesToSet: CreatesPropertyKeys = toNodePropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)

    (hasDynamicProperties && propertiesToSet.overlapsWithDynamicPropertyRead) ||
      propertiesToRead.exists(propertiesToSet.overlaps)
  }

  /*
   * Checks for overlap between what relationship props are read in query graph
   * and what is updated with SET here
   */
  private def setRelPropertyOverlap(propertiesToRead: Set[PropertyKeyName], hasDynamicProperties: Boolean): Boolean = {
    @tailrec
    def toRelPropertyPattern(patterns: Seq[MutatingPattern], acc: CreatesPropertyKeys): CreatesPropertyKeys = {

      def extractPropertyKey(patterns: Seq[SetMutatingPattern]): CreatesPropertyKeys = patterns.collect {
        case SetRelationshipPropertyPattern(_, key, _) => CreatesKnownPropertyKeys(key)
        case SetRelationshipPropertiesPattern(_, items) => CreatesKnownPropertyKeys(items.map(_._1).toSet)
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
        case SetPropertiesPattern(_, items) => CreatesPropertyKeys(items.map(_._2):_*)
        case SetPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      }.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys)(_ + _)

      if (patterns.isEmpty) acc
      else patterns.head match {
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
        case SetPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
        case SetRelationshipPropertyPattern(_, key, _) =>
          toRelPropertyPattern(patterns.tail, acc + CreatesKnownPropertyKeys(key))
        case SetRelationshipPropertiesPattern(_, items) =>
          toRelPropertyPattern(patterns.tail, acc + CreatesKnownPropertyKeys(items.map(_._1).toSet))
        case SetPropertiesPattern(_, items) =>
          toRelPropertyPattern(patterns.tail, acc + CreatesPropertyKeys(items.map(_._2):_*))
        case MergeNodePattern(_, _, onCreate, onMatch) =>
          toRelPropertyPattern(patterns.tail, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
          toRelPropertyPattern(patterns.tail, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case _ => toRelPropertyPattern(patterns.tail, acc)
      }
    }

    val propertiesToSet = toRelPropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)

    (hasDynamicProperties && propertiesToSet.overlapsWithDynamicPropertyRead) ||
      propertiesToRead.exists(propertiesToSet.overlaps)
  }

  private def deleteExpressions = mutatingPatterns.collect {
    case p: DeleteExpression => p
  }

  private def hasDeleteExpressions = mutatingPatterns.exists {
    case _: DeleteExpression => true
    case _ => false
  }

  private def removeLabelPatterns = mutatingPatterns.collect {
    case p: RemoveLabelPattern => p
  }

  private def hasRemoveLabelPatterns = mutatingPatterns.exists {
    case _: RemoveLabelPattern => true
    case _ => false
  }

  private def hasSetLabelPatterns = mutatingPatterns.exists {
    case _: SetLabelPattern => true
    case _ => false
  }

  private def hasSetNodePropertyPatterns = mutatingPatterns.exists {
    case _: SetNodePropertyPattern => true
    case _: SetNodePropertiesFromMapPattern => true
    case _ => false
  }

  def mergeQueryGraph: Option[QueryGraph] = mutatingPatterns.collectFirst {
    case c: MergePattern => c.matchGraph
  }
}
