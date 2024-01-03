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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.Identifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.StableIdentifier
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.UnstableIdentifier
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

object QgWithLeafInfo {

  sealed trait Identifier {
    def variable: LogicalVariable

    def isStable: Boolean
  }

  case class StableIdentifier(override val variable: LogicalVariable) extends Identifier {
    override def isStable: Boolean = true
  }

  case class UnstableIdentifier(override val variable: LogicalVariable) extends Identifier {
    override def isStable: Boolean = false
  }

  /**
   * @return a QgWithInfo, where there is no stable identifier. Moreover all variables are assumed to be leaves.
   */
  def qgWithNoStableIdentifierAndOnlyLeaves(qg: QueryGraph, isReturningProjection: Boolean = false): QgWithLeafInfo =
    QgWithLeafInfo(qg, Set.empty, qg.allCoveredIds, None, isReturningProjection)

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
 * @param isTerminatingProjection  whether this is the projection of the last RETURN in the query.
 */
case class QgWithLeafInfo(
  private val solvedQg: QueryGraph,
  private val stablySolvedPredicates: Set[Predicate],
  private val unstableLeaves: Set[LogicalVariable],
  private val stableIdentifier: Option[StableIdentifier],
  isTerminatingProjection: Boolean
) {

  /**
   * We exclude all stably solved predicates from the eagerness analysis.
   * These are the predicates solved by the leaf plan that solves the stable identifier.
   */
  val queryGraph: QueryGraph = solvedQg.removePredicates(stablySolvedPredicates)

  def hasUnstableLeaves: Boolean = unstableLeaves.nonEmpty

  lazy val unstablePatternNodes: Set[LogicalVariable] =
    queryGraph.allPatternNodesRead -- stableIdentifier.map(_.variable).filterNot(unstableLeaves.contains)

  lazy val unstablePatternRelationships: Set[PatternRelationship] =
    queryGraph.allPatternRelationshipsRead.filterNot(rel => stableIdentifier.exists(i => i.variable == rel.variable))

  lazy val patternNodes: Set[Identifier] = {
    val unstableIdentifiers: Set[Identifier] = unstablePatternNodes.map(UnstableIdentifier)
    val maybeStableIdentifier = stableIdentifier.filter(i => queryGraph.patternNodes.contains(i.variable))
    unstableIdentifiers ++ maybeStableIdentifier
  }

  lazy val leafPatternNodes: Set[Identifier] = {
    patternNodes.filter {
      case UnstableIdentifier(name) => unstableLeaves.contains(name)
      case _: StableIdentifier      => true
    }
  }

  val entityArguments: (SemanticTable => Set[LogicalVariable]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      val nonEntityArguments = queryGraph.selections.predicates
        .flatMap(_.expr.dependencies)
        // find expressions that we know for certain are not entities
        .filterNot(semanticTable.typeFor(_).couldBe(CTNode, CTRelationship))

      // Remove nonEntityArguments from argumentIds to avoid being eager on simple projections like `WITH a.prop AS prop`,
      queryGraph.argumentIds -- nonEntityArguments
    })

  val nonArgumentPatternNodes: (SemanticTable => Set[Identifier]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      patternNodes.filterNot(node => entityArguments(semanticTable).contains(node.variable))
    })

  val patternNodesAndArguments: (SemanticTable => Set[Identifier]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      patternNodes ++ entityArguments(semanticTable).map(UnstableIdentifier)
    })

  val patternRelationshipsAndArguments: (SemanticTable => Set[Identifier]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      patternRelationships ++ entityArguments(semanticTable).map(UnstableIdentifier)
    })

  lazy val patternRelationships: Set[Identifier] = {
    val unstableIdentifiers: Set[Identifier] =
      unstablePatternRelationships.map(rel => UnstableIdentifier(rel.variable))
    val maybeStableIdentifier =
      stableIdentifier.filter(i => queryGraph.patternRelationships.exists(rel => i.variable == rel.variable))
    unstableIdentifiers ++ maybeStableIdentifier
  }

  val allKnownUnstableNodeLabelsFor: (Identifier => Set[LabelName]) with CachedFunction =
    CachedFunction((identifier: Identifier) => {
      queryGraph.allPossibleLabelsOnNode(identifier.variable)
    })

  val allPossibleUnstableRelTypesFor: (Identifier => Set[RelTypeName]) with CachedFunction =
    CachedFunction((identifier: Identifier) => {
      queryGraph.allPossibleTypesOnRel(identifier.variable)
    })

  val allKnownUnstablePropertiesFor: (Identifier => Set[PropertyKeyName]) with CachedFunction =
    CachedFunction((identifier: Identifier) => {
      queryGraph.allKnownPropertiesOnIdentifier(identifier.variable)
    })

  val allKnownUnstableNodeLabels: (SemanticTable => Set[LabelName]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      patternNodesAndArguments(semanticTable).flatMap(allKnownUnstableNodeLabelsFor)
    })

  val allKnownUnstableNodeProperties: (SemanticTable => Set[PropertyKeyName]) with CachedFunction =
    CachedFunction((semanticTable: SemanticTable) => {
      patternNodesAndArguments(semanticTable).flatMap(allKnownUnstablePropertiesFor) ++ irExpressionProperties
    })

  val allKnownUnstableRelProperties: (SemanticTable => Set[PropertyKeyName]) with CachedFunction = CachedFunction(
    (semanticTable: SemanticTable) => {
      patternRelationshipsAndArguments(semanticTable).flatMap(
        allKnownUnstablePropertiesFor
      ) ++ irExpressionProperties
    }
  )

  private lazy val irExpressionProperties: Set[PropertyKeyName] = {
    queryGraph.folder.findAllByClass[IRExpression].flatMap {
      _.folder.findAllByClass[PropertyKeyName]
    }.toSet
  }

  def allPossibleLabelsOnNode(node: LogicalVariable): Set[LabelName] =
    solvedQg.allPossibleLabelsOnNode(node)

}
