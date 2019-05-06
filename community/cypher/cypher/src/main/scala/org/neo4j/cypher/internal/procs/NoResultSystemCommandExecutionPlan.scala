/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{EmptyQuerySubscription, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.helpers.collection.Iterators
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

/**
  * Execution plan for performing schema writes, i.e. creating or dropping indexes and constraints.
  *
  * @param name        A name of the schema write
  * @param schemaWrite The actual schema write to perform
  */
case class NoResultSystemCommandExecutionPlan()
  extends ExecutionPlan {

  override def run(ctx: QueryContext,
                   doProfile: Boolean,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {

    val runtimeResult = NoRuntimeResult(subscriber)
    runtimeResult
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

case class NoRuntimeResult(subscriber: QuerySubscriber) extends EmptyQuerySubscription(subscriber) with RuntimeResult {

  override def fieldNames(): Array[String] = Array.empty

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {}

  override def queryStatistics(): QueryStatistics = QueryStatistics()

  override def isIterable: Boolean = true

  override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] = Iterators.emptyResourceIterator()

  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE
}