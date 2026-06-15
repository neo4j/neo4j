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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.plandescription.Arguments.AvailableProcessors
import org.neo4j.cypher.internal.plandescription.Arguments.AvailableWorkers
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.ByteCode
import org.neo4j.cypher.internal.plandescription.Arguments.Comment
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredCallables
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredConstants
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredVariables
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.Distinctness
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.Arguments.IdArg
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingCallables
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingConstants
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingPath
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingPredicate
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingProjectionItems
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingTopology
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingVariables
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingCallables
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingConstants
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingVariables
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersionArgument
import org.neo4j.cypher.internal.plandescription.Arguments.Referenced
import org.neo4j.cypher.internal.plandescription.Arguments.ResultColumns
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeImpl
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.SourceCode
import org.neo4j.cypher.internal.plandescription.Arguments.StringRepresentation
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.Arguments.UsedIndexes
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.util.attribution.Id

object PlanDescriptionArgumentSerializer {

  def serialize(arg: Argument): AnyRef = {
    arg match {
      case Details(info)                                  => info.mkPrettyString(", ").prettifiedString
      case DbHits(value)                                  => Long.box(value)
      case Memory(value)                                  => Long.box(value)
      case GlobalMemory(value)                            => Long.box(value)
      case AvailableWorkers(value)                        => Int.box(value)
      case AvailableProcessors(value)                     => Int.box(value)
      case PageCacheHits(value)                           => Long.box(value)
      case PageCacheMisses(value)                         => Long.box(value)
      case Rows(value)                                    => Long.box(value)
      case Time(value)                                    => Long.box(value)
      case EstimatedRows(effectiveCardinality, _)         => Double.box(effectiveCardinality)
      case Order(providedOrder)                           => providedOrder.prettifiedString
      case Distinctness(distinctness)                     => distinctness.prettifiedString
      case Version(version)                               => version
      case Planner(planner)                               => planner
      case PlannerImpl(plannerName)                       => plannerName
      case plannerVersionArgument: PlannerVersionArgument => plannerVersionArgument.value
      case Runtime(runtime)                               => runtime
      case RuntimeVersion(value)                          => value
      case SourceCode(_, sourceCode)                      => sourceCode
      case ByteCode(_, byteCode)                          => byteCode
      case RuntimeImpl(runtimeName)                       => runtimeName
      case BatchSize(size)                                => Int.box(size)
      case PipelineInfo(pipelineId, fused, markAsSerial) =>
        val fusion = if (fused) "Fused in" else "In"
        val serialString = if (markAsSerial) " serial" else ""
        s"$fusion$serialString Pipeline $pipelineId"
      case StringRepresentation(rep) => rep
      case IdArg(Id(id))             => Int.box(id)
      case arg: UsedIndexes          => arg.stringify
      // working scope details
      case IncomingConstants(value)       => value
      case IncomingVariables(value)       => value
      case IncomingCallables(value)       => value
      case IncomingPath(value)            => value
      case IncomingPredicate(value)       => value
      case IncomingTopology(value)        => value
      case IncomingProjectionItems(value) => value
      case Referenced(value)              => value
      case DeclaredConstants(value)       => value
      case DeclaredVariables(value)       => value
      case DeclaredCallables(value)       => value
      case ResultColumns(value)           => value
      case OutgoingConstants(value)       => value
      case OutgoingVariables(value)       => value
      case OutgoingCallables(value)       => value
      case Comment(value)                 => value

      // Do not add a fallthrough here - we rely on exhaustive checking to ensure
      // that we don't forget to add new types of arguments here
    }
  }
}
