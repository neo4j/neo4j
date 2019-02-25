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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.{BaseReactiveResultTest, IteratorBasedResult, QueryContext}
import org.neo4j.cypher.result.{QueryProfile, QueryResult}
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

class PipeExecutionReactiveResultTest extends BaseReactiveResultTest {

  override def runtimeResult(subscriber: QuerySubscriber, first: Array[AnyValue], more: Array[AnyValue]*): PipeExecutionResult = {
    val fieldNames = (1 to first.length).map(i => s"f$i").toArray
    val results = (first +: more).iterator.map(new ResultRecord(_))
    new PipeExecutionResult(
      IteratorBasedResult(Iterator.empty, Some(results)),
      fieldNames, QueryStateHelper.emptyWith(query = mock[QueryContext](RETURNS_DEEP_STUBS)), QueryProfile.NONE, subscriber)
  }

  class ResultRecord(val fields: Array[AnyValue]) extends QueryResult.Record
}
