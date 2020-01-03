/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.procs

import java.lang
import java.util.Optional

import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{EmptyQuerySubscription, QueryProfile, RuntimeResult}
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Empty result, as produced by a schema write.
  */
case class SchemaWriteRuntimeResult(ctx: QueryContext, subscriber: QuerySubscriber) extends EmptyQuerySubscription(subscriber) with RuntimeResult {

  override def fieldNames(): Array[String] = Array.empty

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def totalAllocatedMemory(): Optional[lang.Long] = Optional.empty()

  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE
}
