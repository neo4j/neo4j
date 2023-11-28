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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.UnstableIdentifier
import org.neo4j.cypher.internal.ir.UpdateGraph.LeafPlansPredicatesResolver
import org.neo4j.cypher.internal.ir.UpdateGraph.LeafPlansPredicatesResolver.LeafPlansWithSolvedPredicates
import org.neo4j.cypher.internal.ir.UpdateGraph.SolvedPredicatesOfOneLeafPlan
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.DeleteOverlaps
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

import scala.annotation.tailrec
import scala.collection.immutable.ListSet

object UpdateGraph {

  /**
   * Callback to obtain the predicates solved by the leaf plan(s) that find the
   * entity with the given name. Grouped by leaf plan.
   *
   * This is not guaranteed to find the leaf plan(s), so it might return
   * NoLeafPlansFound.
   */
  trait LeafPlansPredicatesResolver {
    def apply(entityName: String): LeafPlansWithSolvedPredicates
  }

  object LeafPlansPredicatesResolver {
    sealed trait LeafPlansWithSolvedPredicates

    /**
     * No leaf plans were found
     */
    case object NoLeafPlansFound extends LeafPlansWithSolvedPredicates

    /**
     * Leaf plans were found. Return the solved predicates for each.
     */
    case class LeafPlansFound(solvedPredicatesForLeafPlans: NonEmptyList[SolvedPredicatesOfOneLeafPlan])
        extends LeafPlansWithSolvedPredicates
  }

  /**
   * The predicates solved by a single leaf plan.
   */
  case class SolvedPredicatesOfOneLeafPlan(predicates: Seq[Expression])
}

trait UpdateGraph {

  def mutatingPatterns: Seq[MutatingPattern]

  def readOnly: Boolean = mutatingPatterns.isEmpty

  def containsUpdates: Boolean = !readOnly

  def containsMergeRecursive: Boolean = hasMergeNodePatterns || hasMergeRelationshipPatterns ||
    foreachPatterns.exists(_.innerUpdates.allQGsWithLeafInfo.map(_.queryGraph).exists(_.containsMergeRecursive))

  private def getMaybeQueryGraph: Option[QueryGraph] =
    this match {
      case qg: QueryGraph => Some(qg)
      case _              => None
    }

  /*
   * Finds all nodes being created with CREATE ...
   */
  def createPatterns: Seq[CreatePattern] = mutatingPatterns.collect {
    case p: CreatePattern => p
  }

  def mergeNodePatterns: Seq[MergeNodePattern] = mutatingPatterns.collect {
    case m: MergeNodePattern => m
  }

  def hasMergeNodePatterns: Boolean = mutatingPatterns.exists {
    case _: MergeNodePattern => true
    case _                   => false
  }

  def mergeRelationshipPatterns: Seq[MergeRelationshipPattern] = mutatingPatterns.collect {
    case m: MergeRelationshipPattern => m
  }

  def hasMergeRelationshipPatterns: Boolean = mutatingPatterns.exists {
    case _: MergeRelationshipPattern => true
    case _                           => false
  }

  def foreachPatterns: Seq[ForeachPattern] = mutatingPatterns.collect {
    case p: ForeachPattern => p
  }

  def hasForeachPatterns: Boolean = mutatingPatterns.exists {
    case _: ForeachPattern => true
    case _                 => false
  }

  /*
   * Finds all identifiers being deleted.
   */
  def identifiersToDelete: Set[String] = (deletes flatMap {
    // DELETE n
    case DeleteExpression(expr: LogicalVariable, _) => Set(expr.name)
    // DELETE (n)-[r]-()
    case DeleteExpression(expr: PathExpression, _) => expr.dependencies.map(_.name)
    case _                                         => Set()
  }).toSet

  /*
   * Finds all labels for each node being created
   * CREATE (:A) CREATE (:B:C) would make a Set(Set(A), Set(B,C))
   */
  lazy val createLabels: Set[Set[LabelName]] =
    createPatterns.flatMap(_.nodes).map(_.labels).toSet ++
      mergeNodePatterns.map(_.createNode.labels) ++
      mergeRelationshipPatterns.flatMap(_.createNodes).map(_.labels)

  /*
   * Finds all node properties being created with CREATE ({prop...})
   */
  lazy val createNodeProperties: CreatesPropertyKeys =
    CreatesPropertyKeys(createPatterns.flatMap(_.nodes.flatMap(_.properties)): _*) +
      CreatesPropertyKeys(mergeNodePatterns.flatMap(_.createNode.properties): _*) +
      CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createNodes.flatMap(c => c.properties)): _*)

  /*
   * Finds all rel properties being created with CREATE
   */
  lazy val createRelProperties: CreatesPropertyKeys =
    CreatesPropertyKeys(createPatterns.flatMap(_.relationships.flatMap(_.properties)): _*) +
      CreatesPropertyKeys(mergeRelationshipPatterns.flatMap(_.createRelationships.flatMap(c => c.properties)): _*)

  /*
   * finds all label names being removed on given node, REMOVE a:L
   */
  def labelsToRemoveFromOtherNodes(idName: String): Set[LabelName] = removeLabelPatterns.collect {
    case RemoveLabelPattern(n, labels) if n.name != idName => labels
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

  /**
   * Foreach should have been flattened before calling in here
   */
  def assertNoForeach(): Unit = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !this.hasForeachPatterns,
      "Foreach should be flattened prior to Eagerness Analysis"
    )
  }

  /*
   * Checks if there is overlap between what is being read in the query graph
   * and what is being written here
   */
  def overlaps(
    qgWithInfo: QgWithLeafInfo,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  )(implicit semanticTable: SemanticTable): ListSet[EagernessReason] = {
    if (!containsUpdates) {
      ListSet.empty
    } else {
      assertNoForeach()

      // A MERGE is always on its own in a QG. That's why we pick either the read graph of a MERGE or the qg itself.
      val readQg =
        qgWithInfo.queryGraph.mergeQueryGraph.map(mergeQg => qgWithInfo.copy(solvedQg = mergeQg)).getOrElse(qgWithInfo)

      lazy val unknownReasons = nodeOverlap(readQg) ||
        createRelationshipOverlap(readQg) ||
        setPropertyOverlap(readQg) ||
        deleteOverlapWithMergeIn(qgWithInfo.queryGraph)

      val checkers = Seq(
        deleteOverlap(_, leafPlansPredicatesResolver),
        removeLabelOverlap(_),
        setLabelOverlap(_)
      )

      val reasons = checkers.view
        .flatMap(c => c(readQg))
        .to(ListSet)

      if (reasons.nonEmpty) {
        reasons
      } else if (unknownReasons) {
        ListSet(EagernessReason.Unknown)
      } else {
        ListSet.empty
      }
    }
  }

  /*
   * Determines whether there's an overlap in writes being done here, and reads being done in the given horizon.
   */
  def overlapsHorizon(
    horizon: QueryHorizon,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  )(implicit semanticTable: SemanticTable): ListSet[EagernessReason] = {
    if (!containsUpdates || !horizon.couldContainRead) {
      ListSet.empty
    } else {
      horizon.allQueryGraphs.view.flatMap(overlaps(_, leafPlansPredicatesResolver)).to(ListSet)
    }
  }

  def createsNodes: Boolean = mutatingPatterns.exists {
    case c: CreatePattern if c.nodes.nonEmpty                => true
    case _: MergeNodePattern                                 => true
    case MergeRelationshipPattern(nodesToCreate, _, _, _, _) => nodesToCreate.nonEmpty
    case _                                                   => false
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

    // Only leafpattern nodes will be considered, so a QueryGraph with a QPP or SPP will ignore the QPP and SPP nodes
    val relevantNodes = qgWithInfo.nonArgumentPatternNodes(semanticTable) intersect qgWithInfo.leafPatternNodes
    updatesNodes && relevantNodes.exists { currentNode =>
      lazy val labelsOnCurrentNode = qgWithInfo.allKnownUnstableNodeLabelsFor(currentNode)
      lazy val labelsToRemove = labelsToRemoveFromOtherNodes(currentNode.name)
      val unstableIdentifierNeedsEager = currentNode match {
        case _: StableIdentifier => false
        case _: UnstableIdentifier =>
          val propertiesOnCurrentNode = qgWithInfo.allKnownUnstablePropertiesFor(currentNode)
          val noLabelOrPropOverlap = labelsOnCurrentNode.isEmpty && propertiesOnCurrentNode.isEmpty && tailCreatesNodes

          // MATCH () CREATE/MERGE (...)?
          noLabelOrPropOverlap ||
          // MATCH (A&B|!C) CREATE (:A:B)
          ((labelsOnCurrentNode.nonEmpty || propertiesOnCurrentNode.nonEmpty) && labelAndPropertyExpressionsOverlap(
            qgWithInfo,
            labelsToCreate,
            NodesToCheckOverlap(None, currentNode.name),
            propertiesToCreate
          )) ||
          // MATCH ({prop:42}) CREATE ({prop:...})
          (labelsOnCurrentNode.isEmpty && propertiesOnCurrentNode.exists(propertiesToCreate.overlaps))
      }

      unstableIdentifierNeedsEager ||
      // MATCH (n:A), (m:B) REMOVE n:B
      // MATCH (n:A), (m:A) REMOVE m:A
      (labelsToRemove intersect labelsOnCurrentNode).nonEmpty

    }
  }

  /**
   * Uses an expression evaluator to figure out if we have a label or a property overlap.
   * For example, if we have `CREATE (:A:B{prop:foo})` we need to solve the predicates given labels A, B and prop (and no other labels or properties).
   * For predicates which contains non label expressions or properties we default to true.
   *
   * If we have multiple predicates, we will only have an overlap if all predicates are evaluated to true.
   * For example, if we have `MATCH (n) WHERE n:A AND n:B CREATE (:A)` we don't need to insert an eager since the predicate `(n:B)` will be evaluated to false.
   *
   * @param qgWithInfo
   * @param possibleLabelCombinations A set of all possible combinations of Labels
   * @param nodes                     The nodes we are checking overlaps between
   * @param propertiesToCreate - the created node and property
   * @return
   */
  private def labelAndPropertyExpressionsOverlap(
    qgWithInfo: QgWithLeafInfo,
    possibleLabelCombinations: Set[Set[LabelName]],
    nodes: NodesToCheckOverlap,
    propertiesToCreate: CreatesPropertyKeys
  ): Boolean = {
    val selections =
      Selections(
        qgWithInfo.queryGraph.selections.predicates ++
          qgWithInfo.queryGraph.optionalMatches.flatMap(_.selections.predicates)
      )

    val unstableNodePredicates = selections.predicatesGiven(Set(nodes.matchedNode))

    possibleLabelCombinations.exists { labelsToCreate =>
      val overlap = CreateOverlaps.overlap(unstableNodePredicates, labelsToCreate.map(_.name), propertiesToCreate)
      overlap match {
        case CreateOverlaps.NoPropertyOverlap                                                 => false
        case CreateOverlaps.NoLabelOverlap                                                    => false
        case CreateOverlaps.Overlap(unprocessedExpressions, propertiesOverlap, labelsOverlap) => true
      }
    }
  }

  private case class NodesToCheckOverlap(updatedNode: Option[String], matchedNode: String) {

    def contains(node: String): Boolean =
      updatedNode.contains(node) || matchedNode == node
  }

  // if we do match delete and merge we always need to be eager
  def deleteOverlapWithMergeIn(other: UpdateGraph): Boolean =
    hasDeletes && (other.hasMergeNodePatterns || other.hasMergeRelationshipPatterns)
  // NOTE: As long as we have the conservative eagerness rule for FOREACH we do not need this recursive check
  // || other.foreachPatterns.exists(_.innerUpdates.allQueryGraphs.exists(deleteOverlapWithMergeIn)))

  def getDegreeOverlap(qgWithInfo: QgWithLeafInfo) = {
    val predicates = qgWithInfo.queryGraph.selections.predicates.map(_.expr)
    val getDegreeRelationshipTypes = predicates.collect {
      case getDegree: GetDegree                                     => getDegree.relType
      case hasDegree: HasDegree                                     => hasDegree.relType
      case hasDegreeGreaterThan: HasDegreeGreaterThan               => hasDegreeGreaterThan.relType
      case hasDegreeGreaterThanOrEqual: HasDegreeGreaterThanOrEqual => hasDegreeGreaterThanOrEqual.relType
      case hasDegreeLessThan: HasDegreeLessThan                     => hasDegreeLessThan.relType
      case hasDegreeLessThanOrEqual: HasDegreeLessThanOrEqual       => hasDegreeLessThanOrEqual.relType
    }

    getDegreeRelationshipTypes.nonEmpty && relationshipOverlap(getDegreeRelationshipTypes.flatten, Set.empty)
  }

  /*
   * Checks for overlap between rels being read in the query graph
   * and those being created here
   */
  def createRelationshipOverlap(qgWithInfo: QgWithLeafInfo): Boolean = {
    // MATCH ()-->() CREATE ()-->()
    allRelPatternsWrittenNonEmpty &&
    (getDegreeOverlap(qgWithInfo) ||
      qgWithInfo.patternRelationships.exists(r => {
        val readProps = qgWithInfo.allKnownUnstablePropertiesFor(r)
        val types = qgWithInfo.allPossibleUnstableRelTypesFor(r)
        relationshipOverlap(types, readProps)
      }))
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

      if (patterns.isEmpty) {
        acc
      } else {
        patterns.head match {
          case SetLabelPattern(_, labels) => toLabelPattern(patterns.tail, acc ++ labels)
          case MergeNodePattern(_, _, onCreate, onMatch) =>
            toLabelPattern(patterns.tail, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
          case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
            toLabelPattern(patterns.tail, acc ++ extractLabels(onCreate) ++ extractLabels(onMatch))
          case _ => toLabelPattern(patterns.tail, acc)
        }
      }
    }

    toLabelPattern(mutatingPatterns, Set.empty)
  }

  /*
   * Checks for overlap between labels being read in query graph
   * and labels being updated with SET and MERGE here
   */
  def setLabelOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Seq[EagernessReason] = {
    // For SET label, we even have to look at the arguments for which we don't know if they are a node or not, so we consider HasLabelsOrTypes predicates.
    lazy val overlapWithKnownLabels: Seq[LabelName] = qgWithInfo.patternNodesAndArguments(semanticTable)
      .flatMap(p => qgWithInfo.allKnownUnstableNodeLabelsFor(p).intersect(labelsToSet)).toSeq
    def overlapWithLabelsFunction: Boolean = qgWithInfo.folder.treeExists {
      case f: FunctionInvocation => f.function == Labels
    }
    def overlapWithWildcard: Boolean = qgWithInfo.folder.treeExists {
      case _: HasALabel => true
    }

    if (labelsToSet.nonEmpty && overlapWithKnownLabels.nonEmpty)
      overlapWithKnownLabels.map(EagernessReason.LabelReadSetConflict(_))
    else if (labelsToSet.nonEmpty && overlapWithLabelsFunction)
      labelsToSet.toSeq.map(EagernessReason.LabelReadSetConflict(_))
    else if (labelsToSet.nonEmpty && overlapWithWildcard)
      labelsToSet.toSeq.map(EagernessReason.LabelReadSetConflict(_))
    else if (labelsToSet.nonEmpty && isReturningNode(qgWithInfo, semanticTable))
      labelsToSet.toSeq.map(EagernessReason.LabelReadSetConflict(_))
    else
      Seq.empty
  }

  /*
   * Checks for overlap between what props are read in query graph
   * and what is updated with SET and MERGE here
   */
  def setPropertyOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Boolean = {
    lazy val hasDynamicProperties = qgWithInfo.folder.treeExists {
      case ContainerIndex(_, index) =>
        // if we access by index, foo[0] or foo[&autoIntX] we must be accessing a list and hence we
        // are not accessing a property
        !semanticTable.typeFor(index).is(CTInteger)
    }
    lazy val hasPropertyFunctionRead = this != qgWithInfo.queryGraph && qgWithInfo.queryGraph.folder.treeExists {
      case Properties(expr) if !semanticTable.typeFor(expr).is(CTMap) =>
        true
    }

    val readPropKeys = getReadPropKeys(qgWithInfo)

    setNodePropertyOverlap(
      readPropKeys.nodePropertyKeys,
      hasDynamicProperties,
      hasPropertyFunctionRead,
      isReturningNode(qgWithInfo, semanticTable)
    ) ||
    setRelPropertyOverlap(readPropKeys.relPropertyKeys, hasDynamicProperties, isReturningRel(qgWithInfo, semanticTable))
  }

  private case class ReadPropKeys(nodePropertyKeys: Set[PropertyKeyName], relPropertyKeys: Set[PropertyKeyName])

  private def getReadPropKeys(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): ReadPropKeys = {
    val (readNodePropKeys, readRelPropKeys, readOtherPropKeys) =
      // Don't do this when comparing against self, to avoid finding overlap for e.g. SET n.prop = n.prop + 1
      if (this != qgWithInfo.queryGraph) {
        val readProps = qgWithInfo.queryGraph.mutatingPatterns.folder.findAllByClass[Property]
        val (readNodeProps, readRelOrOtherProps) = readProps.partition(p => semanticTable.typeFor(p.map).is(CTNode))
        val (readRelProps, readOtherProps) =
          readRelOrOtherProps.partition(p => semanticTable.typeFor(p.map).is(CTRelationship))
        val filteredOtherProps = readOtherProps.filterNot(p => semanticTable.typeFor(p.map).is(CTMap))

        (readNodeProps.map(_.propertyKey), readRelProps.map(_.propertyKey), filteredOtherProps.map(_.propertyKey))
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
  def deleteOverlap(
    qgWithInfo: QgWithLeafInfo,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  )(implicit semanticTable: SemanticTable): Seq[EagernessReason] = {
    if (!hasDeletes) {
      return Seq.empty
    }

    val nodesToRead =
      qgWithInfo.unstablePatternNodes ++
        qgWithInfo.queryGraph.argumentIds.filter(semanticTable.typeFor(_).couldBe(CTNode))

    val relsToRead =
      qgWithInfo.queryGraph.allPatternRelationshipsRead.map(_.variable.name) ++
        qgWithInfo.queryGraph.argumentIds.filter(semanticTable.typeFor(_).couldBe(CTRelationship))

    val identifiersToRead = nodesToRead ++ relsToRead

    if (
      (deletesExpressions && identifiersToRead.nonEmpty) ||
      (hasDetachDelete && relsToRead.nonEmpty)
    ) {
      Seq(EagernessReason.Unknown)
    } else {
      val overlaps = (identifiersToDelete intersect identifiersToRead).toSeq
      if (overlaps.nonEmpty) {
        overlaps.map(EagernessReason.ReadDeleteConflict)
      } else {
        deleteLabelExpressionOverlap(qgWithInfo, leafPlansPredicatesResolver)
      }
    }
  }

  /**
   * Checks if any node can have an overlap with the deleted node.
   *
   * @return the nodes which are overlapping, or None if there is no overlap
   */
  private def deleteLabelExpressionOverlap(
    qgWithInfo: QgWithLeafInfo,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  )(implicit semanticTable: SemanticTable): Seq[EagernessReason] = {
    val relevantNodes =
      qgWithInfo.queryGraph.allPatternNodesRead ++
        // Using qgWithInfo.queryGraph.argumentIds here would give many false positives, where a node is an
        // argument, but not further used. Using selections (only), because QueryHorizon.allQueryGraphs
        // puts any expressions into there.
        qgWithInfo.queryGraph.selections.variableDependencies.filter(semanticTable.typeFor(_).couldBe(CTNode))
    val nodesWithLabelOverlap = relevantNodes
      .flatMap(unstableNode => identifiersToDelete.map((unstableNode, _)))
      .filter { case (readNode, deletedNode) =>
        readNode != deletedNode &&
        getDeleteOverlapWithLabelExpression(
          qgWithInfo,
          readNode,
          deletedNode,
          leafPlansPredicatesResolver
        )
      }
      .flatMap { case (unstableNode, _) => Set(unstableNode) }

    if (nodesWithLabelOverlap.nonEmpty) {
      nodesWithLabelOverlap.map(EagernessReason.ReadDeleteConflict).toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Checks if there is any overlap between the unstable node and deleted node.
   * This is done by checking if there is any set of labels which evaluates both the deleted nodes label expression and the unstable nodes label expression
   * to true.
   *
   * E.g
   * Given:
   * unstable node: x
   * deleted node: y
   *
   * Expression                           Return
   * MATCH (x:A), (y:B) DELETE y          true (overlap if we have a node with both label "A" and Label "B")
   * MATCH (x:!A), (y:A) DELETE y         false (both expressions can never be true)
   *
   * @param qgWithInfo query graph
   * @param readNode the node for which to check if there exists overlap with the delete node
   * @param deletedNode the deleted node
   * @return true if there exists any chance of overlap
   */
  private def getDeleteOverlapWithLabelExpression(
    qgWithInfo: QgWithLeafInfo,
    readNode: String,
    deletedNode: String,
    leafPlansPredicatesResolver: LeafPlansPredicatesResolver
  ): Boolean = {
    // For the read node, we must use the predicates that are solved by the leaf plan(s) solving that node.
    // If any of the predicates for one leaf plan overlaps with the deletedNodePredicates, we have to be Eager.
    val readNodePredicates = leafPlansPredicatesResolver(readNode)

    // For the deleted node, we can include all predicates, since they are all applied before the node gets deleted.
    // We collect the predicates from both the readQG (qgWithInfo) and the writeQG (getMaybeQueryGraph).
    // This is not necessarily all predicates, but missing some will only be conservative.
    val selections =
      qgWithInfo.queryGraph.allSelections ++
        getMaybeQueryGraph.map(_.allSelections).getOrElse(Selections.empty)
    val deletedNodePredicates = selections.predicatesGiven(Set(deletedNode))

    // If readNodePredicates == NoLeafPlansFound, we could not find the leaf plan(s) that solve the read node.
    // This happens for instance when we are on the RHS of an Apply.
    // Since we don't know the predicates, we have to be conservative.
    readNodePredicates match {
      case LeafPlansPredicatesResolver.NoLeafPlansFound => true
      case LeafPlansPredicatesResolver.LeafPlansFound(solvedPredicatesForLeafPlans) =>
        solvedPredicatesForLeafPlans.exists[SolvedPredicatesOfOneLeafPlan] {
          case SolvedPredicatesOfOneLeafPlan(readNodePredicatesForLeafPlan) =>
            val overlap = DeleteOverlaps.overlap(readNodePredicatesForLeafPlan, deletedNodePredicates)

            overlap match {
              case DeleteOverlaps.NoLabelOverlap                                 => false
              case DeleteOverlaps.Overlap(unprocessedExpressions, labelsOverlap) => true
            }
        }
    }
  }

  def removeLabelOverlap(qgWithInfo: QgWithLeafInfo)(implicit semanticTable: SemanticTable): Seq[EagernessReason] = {
    lazy val otherLabelsRead = qgWithInfo.allKnownUnstableNodeLabels(semanticTable)
    lazy val overlapWithLabelsFunction = qgWithInfo.folder.treeExists {
      case f: FunctionInvocation => f.function == Labels
    }
    lazy val overlapWithWildcard = qgWithInfo.folder.treeExists {
      case _: HasALabel => true
    }

    val overlappingLabels: Seq[LabelName] = removeLabelPatterns.collect {
      case RemoveLabelPattern(_, labelsToRemove) if overlapWithLabelsFunction || overlapWithWildcard => labelsToRemove
      case RemoveLabelPattern(_, labelsToRemove)
        if labelsToRemove.nonEmpty && isReturningNode(qgWithInfo, semanticTable) =>
        labelsToRemove
      case RemoveLabelPattern(_, labelsToRemove) =>
        // does any other identifier match on the labels I am deleting?
        // MATCH (a:BAR)..(b) REMOVE b:BAR
        labelsToRemove.filter(l => {
          otherLabelsRead(l)
        })
    }.flatten

    if (overlappingLabels.nonEmpty) {
      overlappingLabels.map(EagernessReason.LabelReadRemoveConflict)
    } else {
      Seq.empty
    }
  }

  /*
   * Checks for overlap between what node props are read in query graph
   * and what is updated with SET here (properties added by create/merge directly is handled elsewhere)
   */
  private def setNodePropertyOverlap(
    propertiesToRead: Set[PropertyKeyName],
    hasDynamicProperties: => Boolean,
    hasPropertyFunctionRead: => Boolean,
    isReturningNode: => Boolean
  ): Boolean = {
    def extractPropertyKey(pattern: SetMutatingPattern): CreatesPropertyKeys = pattern match {
      case SetPropertyPattern(_, propertyKeyName, _) =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesKnownPropertyKeys(propertyKeyName)
      case SetPropertiesPattern(_, items) =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesKnownPropertyKeys(items.map(_._1).toSet)
      case _: SetRelationshipPropertyPattern =>
        // Not dealing with relationships here
        CreatesNoPropertyKeys
      case _: SetRelationshipPropertiesPattern =>
        // Not dealing with relationships here
        CreatesNoPropertyKeys
      case SetNodePropertiesFromMapPattern(_, expression, _) =>
        CreatesPropertyKeys(expression)
      case _: SetRelationshipPropertiesFromMapPattern =>
        // Not dealing with relationships here
        CreatesNoPropertyKeys
      case SetPropertiesFromMapPattern(_, expression, _) =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesPropertyKeys(expression)
      case SetNodePropertyPattern(_, propertyKey, _) =>
        CreatesKnownPropertyKeys(propertyKey)
      case SetNodePropertiesPattern(_, items) =>
        CreatesKnownPropertyKeys(items.map(_._1).toSet)
      case _: SetLabelPattern =>
        // We're not dealing with labels here
        CreatesNoPropertyKeys
      case _: RemoveLabelPattern =>
        // We're not dealing with labels here
        CreatesNoPropertyKeys
    }

    val propertiesToSet: CreatesPropertyKeys = mutatingPatterns.collect {
      case smp: SetMutatingPattern => Seq(smp)
      case MergeNodePattern(_, _, onCreate, onMatch) =>
        onCreate ++ onMatch
      case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
        onCreate ++ onMatch
    }.flatten
      .map(extractPropertyKey)
      .reduceOption(_ + _)
      .getOrElse(CreatesNoPropertyKeys)

    (propertiesToSet.overlapsWithDynamicPropertyRead && (isReturningNode || hasDynamicProperties)) ||
    (propertiesToSet.overlapsWithFunctionPropertyRead && hasPropertyFunctionRead) ||
    propertiesToRead.exists(propertiesToSet.overlaps)
  }

  /*
   * Checks for overlap between what relationship props are read in query graph
   * and what is updated with SET here
   */
  private def setRelPropertyOverlap(
    propertiesToRead: Set[PropertyKeyName],
    hasDynamicProperties: => Boolean,
    isReturningRel: => Boolean
  ): Boolean = {
    def extractPropertyKey(pattern: SetMutatingPattern): CreatesPropertyKeys = pattern match {
      case SetPropertyPattern(_, propertyKeyName, _) =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesKnownPropertyKeys(propertyKeyName)
      case SetPropertiesPattern(_, items) =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesKnownPropertyKeys(items.map(_._1).toSet)
      case SetRelationshipPropertyPattern(_, propertyKey, _) =>
        CreatesKnownPropertyKeys(propertyKey)
      case SetRelationshipPropertiesPattern(_, items) =>
        CreatesKnownPropertyKeys(items.map(_._1).toSet)
      case _: SetNodePropertiesFromMapPattern =>
        // Not dealing with nodes here
        CreatesNoPropertyKeys
      case SetRelationshipPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      case SetPropertiesFromMapPattern(_, expression, _)             =>
        // Not sure whether we're setting on a node or rel, we have to include it to be safe
        CreatesPropertyKeys(expression)
      case _: SetNodePropertyPattern =>
        // Not dealing with nodes here
        CreatesNoPropertyKeys
      case _: SetNodePropertiesPattern =>
        // Not dealing with nodes here
        CreatesNoPropertyKeys
      case _: SetLabelPattern =>
        // We're not dealing with labels here
        CreatesNoPropertyKeys
      case _: RemoveLabelPattern =>
        // We're not dealing with labels here
        CreatesNoPropertyKeys
    }

    val propertiesToSet: CreatesPropertyKeys = mutatingPatterns.collect {
      case smp: SetMutatingPattern => Seq(smp)
      case MergeNodePattern(_, _, onCreate, onMatch) =>
        onCreate ++ onMatch
      case MergeRelationshipPattern(_, _, _, onCreate, onMatch) =>
        onCreate ++ onMatch
    }.flatten
      .map(extractPropertyKey)
      .reduceOption(_ + _)
      .getOrElse(CreatesNoPropertyKeys)

    (propertiesToSet.overlapsWithDynamicPropertyRead && (isReturningRel || hasDynamicProperties)) ||
    propertiesToRead.exists(propertiesToSet.overlaps)
  }

  private def deletes = mutatingPatterns.collect {
    case p: DeleteExpression => p
  }

  private def hasDeletes = mutatingPatterns.exists {
    case _: DeleteExpression => true
    case _                   => false
  }

  def deletesExpressions: Boolean = deletes.exists {
    // `DELETE expression` deletes something without the variable name
    case DeleteExpression(expr, _) if !expr.isInstanceOf[LogicalVariable] && !expr.isInstanceOf[PathExpression] => true
    case _                                                                                                      => false
  }

  def hasDetachDelete: Boolean = deletes.exists {
    // DETACH DELETE deletes relationships without their variable name
    case DeleteExpression(_, true) => true
    case _                         => false
  }

  private def removeLabelPatterns = mutatingPatterns.collect {
    case p: RemoveLabelPattern => p
  }

  private def hasRemoveLabelPatterns = mutatingPatterns.exists {
    case _: RemoveLabelPattern => true
    case _                     => false
  }

  private def hasSetLabelPatterns = mutatingPatterns.exists {
    case _: SetLabelPattern => true
    case _                  => false
  }

  private def hasSetNodePropertyPatterns = mutatingPatterns.exists {
    case _: SetNodePropertyPattern          => true
    case _: SetNodePropertiesFromMapPattern => true
    case _                                  => false
  }

  private def isReturningNode(qgWithInfo: QgWithLeafInfo, semanticTable: SemanticTable): Boolean = {
    // Note: We are confusingly checking the selections, since
    // QueryHorizon.getQueryGraphFromDependingExpressions stores all depending expressions in there.
    // We don't have access to the horizon itself in here, so this is a hack to get this information from the query graph instead.
    qgWithInfo.isTerminatingProjection && qgWithInfo.queryGraph.selections.predicates.map(_.expr).exists {
      // Since IR construction does some rewriting, we don't have type information on all expressions, including
      // AndedPropertyInequalities, which is a subtype of BooleanExpression, which can never be a Node.
      // This extra case avoids some unnecessary Eagers.
      case _: BooleanExpression => false
      case expr                 => semanticTable.typeFor(expr).couldBe(CTNode)
    }
  }

  private def isReturningRel(qgWithInfo: QgWithLeafInfo, semanticTable: SemanticTable): Boolean = {
    // Note: We are confusingly checking the selections, since
    // QueryHorizon.getQueryGraphFromDependingExpressions stores all depending expressions in there.
    // We don't have access to the horizon itself in here, so this is a hack to get this information from the query graph instead.
    qgWithInfo.isTerminatingProjection && qgWithInfo.queryGraph.selections.predicates.map(_.expr).exists {
      // Since IR construction does some rewriting, we don't have type information on all expressions, including
      // AndedPropertyInequalities, which is a subtype of BooleanExpression, which can never be a Relationship.
      // This extra case avoids some unnecessary Eagers.
      case _: BooleanExpression => false
      case expr                 => semanticTable.typeFor(expr).couldBe(CTRelationship)
    }
  }

  def mergeQueryGraph: Option[QueryGraph] = mutatingPatterns.collectFirst {
    case c: MergePattern => c.matchGraph
  }
}
