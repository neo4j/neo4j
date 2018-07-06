/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.parallel

case class DataPoint(id: Long,
                     upstreamId: Long,
                     queryId: Int,
                     schedulingThreadId: Long,
                     scheduledTime: Long,
                     executionThreadId: Long,
                     startTime: Long,
                     stopTime: Long,
                     task: Task) {

  def withTimeZero(t0: Long): DataPoint =
    DataPoint(id,
      upstreamId,
      queryId,
      schedulingThreadId,
      scheduledTime - t0,
      executionThreadId,
      startTime - t0,
      stopTime - t0,
      task)
}

/**
  * Write data points to somewhere.
  */
trait DataPointWriter {

  /**
    * Write (e.g., log) this tracing data point
    */
  def write(dataPoint: DataPoint): Unit

  /**
    * Flush buffered data points
    */
  def flush(): Unit
}

