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

import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter will
 */
object MergeRemoteBatchPropertiesRewriter extends Rewriter with TopDownMergeableRewriter {

  override val innerRewriter: Rewriter = {
    Rewriter.lift {
      case RemoteBatchProperties(
          remoteBatchPropertiesWithFilter: RemoteBatchPropertiesWithFilter,
          properties: Set[LogicalProperty]
        ) =>
        remoteBatchPropertiesWithFilter.copy(properties = remoteBatchPropertiesWithFilter.properties ++ properties)(
          SameId(remoteBatchPropertiesWithFilter.id)
        )

      case remoteBatchPropertiesWithFilter @ RemoteBatchPropertiesWithFilter(
          remoteBatchProperties: RemoteBatchProperties,
          _,
          properties
        ) =>
        remoteBatchPropertiesWithFilter.copy(
          properties = properties ++ remoteBatchProperties.properties,
          source = remoteBatchProperties.source
        )(SameId(remoteBatchPropertiesWithFilter.id))

      case remoteBatchPropertiesWithFilterTop @ RemoteBatchPropertiesWithFilter(
          remoteBatchPropertiesWithFilterUnder: RemoteBatchPropertiesWithFilter,
          predicates,
          properties
        ) =>
        remoteBatchPropertiesWithFilterTop.copy(
          properties = properties ++ remoteBatchPropertiesWithFilterUnder.properties,
          predicates = predicates ++ remoteBatchPropertiesWithFilterUnder.predicates,
          source = remoteBatchPropertiesWithFilterUnder.source
        )(SameId(remoteBatchPropertiesWithFilterTop.id))

    }
  }

  private val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
