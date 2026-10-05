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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteNodeIndexWriteOperators
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteDirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteDirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteMergeUniqueNode
import org.neo4j.cypher.internal.logical.plans.RemoteNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteUndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteUndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrites [[NodeIndexSeek]] and [[NodeUniqueIndexSeek]] plans into [[RemoteNodeIndexSeek]] and
 * [[RemoteNodeUniqueIndexSeek]] plans respectively for SHARDED databases. The relationship index seeks
 * [[DirectedRelationshipIndexSeek]], [[UndirectedRelationshipIndexSeek]],
 * [[DirectedRelationshipUniqueIndexSeek]] and [[UndirectedRelationshipUniqueIndexSeek]] are likewise
 * rewritten into their [[RemoteDirectedRelationshipIndexSeek]], [[RemoteUndirectedRelationshipIndexSeek]],
 * [[RemoteDirectedRelationshipUniqueIndexSeek]] and [[RemoteUndirectedRelationshipUniqueIndexSeek]]
 * counterparts.
 * [[MergeUniqueNode]] is likewise rewritten into [[RemoteMergeUniqueNode]], gated by the
 * [[RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE]] write config, since it is always a write operator.
 * Currently supported only for read-only queries.
 * Additionally, only seeks with arguments are rewritten: the remote operators batch index queries across
 * argument rows, and a seek with no arguments is a single invocation with nothing to batch.
 * Node and relationship seeks are gated independently via [[rewriteNodes]] and [[rewriteRelationships]] so
 * that each remote index kind can be enabled separately as runtime support lands.
 */
case class RemoteIndexSeekRewriter(
  remoteNodeIndexWriteOperatorsConfig: Set[RemoteNodeIndexWriteOperators],
  rewriteNodes: Boolean,
  rewriteRelationships: Boolean
) extends Rewriter {

  override def apply(plan: AnyRef): AnyRef = plan match {
    case lp: LogicalPlan => instance(
        readOnlyQuery = lp.readOnly,
        insideCallInTransactionsIds = callInTransactionIds(lp)
      ).apply(lp)
    case other => other
  }

  private def allIds(plan: LogicalPlan): Set[Id] =
    plan.folder.treeCollect {
      case p: LogicalPlan => p.id
    }.toSet

  /**
   * Collect all the ids of the logical plan operators that are within a CALL IN TRANSACTIONS, that is,
   * on the RHS of a TransactionApply or TransactionForEach
   */
  private def callInTransactionIds(plan: LogicalPlan): Set[Id] = {
    plan.folder.treeFold(Set.empty[Id]) {
      case cit @ (_: TransactionApply | _: TransactionForeach) =>
        acc => SkipChildren(acc ++ callInTransactionIds(cit.left) ++ allIds(cit.right))
    }
  }

  private def rewriteForWriteQuery(requiredConfig: RemoteNodeIndexWriteOperators, inCallInTx: Boolean): Boolean =
    remoteNodeIndexWriteOperatorsConfig.contains(requiredConfig) &&
      (!inCallInTx || remoteNodeIndexWriteOperatorsConfig.contains(RemoteNodeIndexWriteOperators.CALL_IN_TRANSACTIONS))

  private def instance(readOnlyQuery: Boolean, insideCallInTransactionsIds: Set[Id]): Rewriter = topDown(Rewriter.lift {
    case seek @ NodeIndexSeek(
        idName,
        label,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )
      if argumentIds.nonEmpty && rewriteNodes && (readOnlyQuery || rewriteForWriteQuery(
        RemoteNodeIndexWriteOperators.NON_LOCKING,
        insideCallInTransactionsIds.contains(seek.id)
      )) =>
      RemoteNodeIndexSeek(
        idName,
        label,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )(SameId(seek.id))

    case seek @ NodeUniqueIndexSeek(
        idName,
        label,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )
      if argumentIds.nonEmpty && rewriteNodes && (readOnlyQuery || rewriteForWriteQuery(
        RemoteNodeIndexWriteOperators.UNIQUE_INDEX_LOCKING,
        insideCallInTransactionsIds.contains(seek.id)
      )) =>
      RemoteNodeUniqueIndexSeek(
        idName,
        label,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )(SameId(seek.id))

    case merge @ MergeUniqueNode(
        idName,
        label,
        properties,
        seekExpressions,
        argumentIds,
        indexOrder,
        indexType,
        onMatchProperties,
        onCreateProperties
      )
      if argumentIds.nonEmpty && rewriteNodes && rewriteForWriteQuery(
        RemoteNodeIndexWriteOperators.MERGE_UNIQUE_NODE,
        insideCallInTransactionsIds.contains(merge.id)
      ) =>
      RemoteMergeUniqueNode(
        idName,
        label,
        properties,
        seekExpressions,
        argumentIds,
        indexOrder,
        indexType,
        onMatchProperties,
        onCreateProperties
      )(SameId(merge.id))

    case seek @ DirectedRelationshipIndexSeek(
        idName,
        startNode,
        endNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      ) if argumentIds.nonEmpty && rewriteRelationships && readOnlyQuery =>
      RemoteDirectedRelationshipIndexSeek(
        idName,
        startNode,
        endNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )(SameId(seek.id))

    case seek @ UndirectedRelationshipIndexSeek(
        idName,
        leftNode,
        rightNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      ) if argumentIds.nonEmpty && rewriteRelationships && readOnlyQuery =>
      RemoteUndirectedRelationshipIndexSeek(
        idName,
        leftNode,
        rightNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      )(SameId(seek.id))

    case seek @ DirectedRelationshipUniqueIndexSeek(
        idName,
        startNode,
        endNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType
      ) if argumentIds.nonEmpty && rewriteRelationships && readOnlyQuery =>
      RemoteDirectedRelationshipUniqueIndexSeek(
        idName,
        startNode,
        endNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType
      )(SameId(seek.id))

    case seek @ UndirectedRelationshipUniqueIndexSeek(
        idName,
        leftNode,
        rightNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType
      ) if argumentIds.nonEmpty && rewriteRelationships && readOnlyQuery =>
      RemoteUndirectedRelationshipUniqueIndexSeek(
        idName,
        leftNode,
        rightNode,
        typeToken,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType
      )(SameId(seek.id))
  })
}
