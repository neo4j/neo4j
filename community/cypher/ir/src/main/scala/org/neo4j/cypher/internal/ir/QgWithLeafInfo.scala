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
 * @param solvedQg         The query graph that has already been solved by some plan.
 * @param unstableLeaves   The unstable leaves of the considered plan.
 * @param stableIdentifier The identifier of the node found in the stable iterator.
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

  lazy val nonArgumentPatternNodes: Set[Identifier] = {
    patternNodes.filterNot(node => queryGraph.argumentIds.contains(node.name))
  }

  lazy val patternNodesAndArguments: Set[Identifier] = {
    patternNodes ++ queryGraph.argumentIds.map(UnstableIdentifier)
  }

  lazy val patternRelationshipsAndArguments: Set[Identifier] = {
    patternRelationships ++ queryGraph.argumentIds.map(UnstableIdentifier)
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

  lazy val allKnownUnstableNodeLabels: Set[LabelName] = {
    patternNodesAndArguments.flatMap(allKnownUnstableNodeLabelsFor)
  }

  lazy val allKnownUnstableNodeProperties: Set[PropertyKeyName] = {
    patternNodesAndArguments.flatMap(allKnownUnstablePropertiesFor) ++ patternExpressionProperties
  }

  lazy val allKnownUnstableRelProperties: Set[PropertyKeyName] = {
    patternRelationshipsAndArguments.flatMap(allKnownUnstablePropertiesFor) ++ patternExpressionProperties
  }

  private lazy val patternExpressionProperties: Set[PropertyKeyName] = {
    (queryGraph.findByAllClass[PatternComprehension] ++ queryGraph.findByAllClass[PatternExpression]).flatMap {
      _.findByAllClass[PropertyKeyName]
    }.toSet
  }
}
