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

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.RemoteNodeUniqueIndexSeek
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrites [[NodeIndexSeek]] and [[NodeUniqueIndexSeek]] plans into [[RemoteNodeIndexSeek]] and
 * [[RemoteNodeUniqueIndexSeek]] plans respectively for SHARDED databases.
 * Currently supported only for read-only queries.
 * Additionally, only seeks with arguments are rewritten: the remote operators batch index queries across
 * argument rows, and a seek with no arguments is a single invocation with nothing to batch.
 */
case object RemoteIndexSeekRewriter extends Rewriter {

  override def apply(plan: AnyRef): AnyRef = plan match {
    case lp: LogicalPlan if lp.readOnly => instance(lp)
    case other                          => other
  }

  private val instance: Rewriter = topDown(Rewriter.lift {
    case seek @ NodeIndexSeek(
        idName,
        label,
        properties,
        valueExpr,
        argumentIds,
        indexOrder,
        indexType,
        supportPartitionedScan
      ) if argumentIds.nonEmpty =>
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
      ) if argumentIds.nonEmpty =>
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
  })
}
