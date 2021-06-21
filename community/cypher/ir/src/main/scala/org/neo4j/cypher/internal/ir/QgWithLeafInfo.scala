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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.Identifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.UnstableIdentifier
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship

object QgWithLeafInfo {

  sealed trait Identifier {
    def name: String

    def isIdStable: Boolean
  }

  case class StableIdentifier(override val name: String,
                              override val isIdStable: Boolean) extends Identifier

  case class UnstableIdentifier(override val name: String) extends Identifier {
    override def isIdStable: Boolean = false
  }

  /**
   * @return a QgWithInfo, where there is no stable identifier. Moreover all variables are assumed to be leaves.
   */
  def qgWithNoStableIdentifierAndOnlyLeaves(qg: QueryGraph): QgWithLeafInfo =
    QgWithLeafInfo(qg, Set.empty, qg.allCoveredIds, None)

}

/**
 * Encapsulates a query graph with info necessary for certain eager-analysis.
 * Specifically, this includes information about the leaves used to plan the query graph
 * and their stability.
 *
 * @param solvedQg               The query graph that has already been solved by some plan.
 * @param stablySolvedPredicates The predicates solved by the leaf plan that solves the stable identifier.
 * @param unstableLeaves         The unstable leaves of the considered plan.
 * @param stableIdentifier       The identifier of the node found in the stable iterator.
 */
case class QgWithLeafInfo(private val solvedQg: QueryGraph,
                          private val stablySolvedPredicates: Set[Predicate],
                          private val unstableLeaves: Set[String],
                          private val stableIdentifier: Option[StableIdentifier]) {

  /**
   * We exclude all stably solved predicates from the eagerness analysis.
   * These are the predicates solved by the leaf plan that solves the stable identifier.
   */
  val queryGraph: QueryGraph = solvedQg.removePredicates(stablySolvedPredicates)

  def hasUnstableLeaves: Boolean = unstableLeaves.nonEmpty

  lazy val unstablePatternNodes: Set[String] = queryGraph.allPatternNodesRead -- stableIdentifier.map(_.name)

  lazy val unstablePatternRelationships: Set[PatternRelationship] = queryGraph.allPatternRelationshipsRead.filterNot(rel => stableIdentifier.exists(i => i.name == rel.name))

  lazy val patternNodes: Set[Identifier] = {
    val unstableIdentifiers: Set[Identifier] = unstablePatternNodes.map(UnstableIdentifier)
    val maybeStableIdentifier = stableIdentifier.filter(i => queryGraph.patternNodes.contains(i.name))
    unstableIdentifiers ++ maybeStableIdentifier
  }

  lazy val leafPatternNodes: Set[Identifier] = {
    patternNodes.filter {
      case UnstableIdentifier(name) => unstableLeaves.contains(name)
      case _: StableIdentifier => true
    }
  }

  // We expect that the semanticTable will always be the same for these calls, so if we computed it once we can reuse the result for the following calls.
  // Ultimately we'd like to use something like CachedFunction, however, using the SemanticTable as cache-key is surprisingly expensive and ultimately unnecessary.
  private var entityArgumentsCached: Option[Set[String]] = None
  private def entityArguments(semanticTable: SemanticTable): Set[String] = {
    if (entityArgumentsCached.isEmpty) {
      entityArgumentsCached = Some(queryGraph.argumentIds.filter(couldBeEntity(semanticTable, _)))
    }
    entityArgumentsCached.get
  }

  private var nonArgumentPatternNodesCached: Option[Set[Identifier]] = None
  def nonArgumentPatternNodes(semanticTable: SemanticTable): Set[Identifier] = {
    if (nonArgumentPatternNodesCached.isEmpty) {
      nonArgumentPatternNodesCached = Some(patternNodes.filterNot(node => entityArguments(semanticTable).contains(node.name)))
    }
    nonArgumentPatternNodesCached.get
  }

  private var patternNodesAndArgumentsCached: Option[Set[Identifier]] = None
  def patternNodesAndArguments(semanticTable: SemanticTable): Set[Identifier] = {
    if (patternNodesAndArgumentsCached.isEmpty) {
      patternNodesAndArgumentsCached = Some(patternNodes ++ entityArguments(semanticTable).map(UnstableIdentifier))
    }
    patternNodesAndArgumentsCached.get
  }

  private var patternRelationshipsAndArgumentsCached: Option[Set[Identifier]] = None
  def patternRelationshipsAndArguments(semanticTable: SemanticTable): Set[Identifier] = {
    if (patternRelationshipsAndArgumentsCached.isEmpty) {
      patternRelationshipsAndArgumentsCached = Some(patternRelationships ++ entityArguments(semanticTable).map(UnstableIdentifier))
    }
    patternRelationshipsAndArgumentsCached.get
  }

  lazy val patternRelationships: Set[Identifier] = {
    val unstableIdentifiers: Set[Identifier] = unstablePatternRelationships.map(rel => UnstableIdentifier(rel.name))
    val maybeStableIdentifier = stableIdentifier.filter(i => queryGraph.patternRelationships.exists(rel => i.name == rel.name))
    unstableIdentifiers ++ maybeStableIdentifier
  }

  def allKnownUnstableNodeLabelsFor(identifier: Identifier): Set[LabelName] = CachedFunction((identifier:Identifier) => {
    if (identifier.isIdStable) Set.empty[LabelName]
    else queryGraph.allPossibleLabelsOnNode(identifier.name)
  })(identifier)

  def allPossibleUnstableRelTypesFor(identifier: Identifier): Set[RelTypeName] = CachedFunction((identifier:Identifier) => {
    if (identifier.isIdStable) Set.empty[RelTypeName]
    else queryGraph.allPossibleTypesOnRel(identifier.name)
  })(identifier)

  def allKnownUnstablePropertiesFor(identifier: Identifier): Set[PropertyKeyName] = CachedFunction((identifier:Identifier) => {
    if (identifier.isIdStable) Set.empty[PropertyKeyName]
    else queryGraph.allKnownPropertiesOnIdentifier(identifier.name)
  })(identifier)

  private var allKnownUnstableNodeLabelsCached: Option[Set[LabelName]] = None
  def allKnownUnstableNodeLabels(semanticTable: SemanticTable): Set[LabelName] = {
    if (allKnownUnstableNodeLabelsCached.isEmpty) {
      allKnownUnstableNodeLabelsCached = Some(patternNodesAndArguments(semanticTable).flatMap(allKnownUnstableNodeLabelsFor))
    }
    allKnownUnstableNodeLabelsCached.get
  }

  private var allKnownUnstableNodePropertiesCached: Option[Set[PropertyKeyName]] = None
  def allKnownUnstableNodeProperties(semanticTable: SemanticTable): Set[PropertyKeyName] = {
    if (allKnownUnstableNodePropertiesCached.isEmpty) {
      allKnownUnstableNodePropertiesCached = Some(patternNodesAndArguments(semanticTable).flatMap(allKnownUnstablePropertiesFor) ++ patternExpressionProperties)
    }
    allKnownUnstableNodePropertiesCached.get
  }

  private var allKnownUnstableRelPropertiesCached: Option[Set[PropertyKeyName]] = None
  def allKnownUnstableRelProperties(semanticTable: SemanticTable): Set[PropertyKeyName] = {
    if (allKnownUnstableRelPropertiesCached.isEmpty) {
      allKnownUnstableRelPropertiesCached = Some(patternRelationshipsAndArguments(semanticTable).flatMap(allKnownUnstablePropertiesFor) ++ patternExpressionProperties)
    }
    allKnownUnstableRelPropertiesCached.get
  }

  private lazy val patternExpressionProperties: Set[PropertyKeyName] = {
    (queryGraph.findAllByClass[PatternComprehension] ++ queryGraph.findAllByClass[PatternExpression]).flatMap {
      _.findAllByClass[PropertyKeyName]
    }.toSet
  }

  /**
   * Checks whether the given expression could be of type `CTNode` or `CTRelationship`.
   */
  private def couldBeEntity(semanticTable: SemanticTable, variable: String): Boolean = {
    semanticTable.getOptionalActualTypeFor(variable) match {
      case Some(actualType) =>
        actualType.contains(CTNode) || actualType.contains(CTRelationship) || actualType.contains(CTPath)
      case None =>
        // No type information available, we have to be conservative
        true
    }
  }
}
