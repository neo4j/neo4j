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

import java.util.concurrent.TimeUnit

/**
  * DataPointWriter which accepts DataPoints, formats as CSV, and prints to std out.
  */
class CsvStdOutDataWriter extends DataPointWriter {

  import CsvStdOutDataWriter._

  private val sb = new StringBuilder(HEADER)

  def flush(): Unit = {
    val result = sb.result()
    sb.clear()
    sb ++= HEADER
    println(result)
  }

  override def write(dp: DataPoint): Unit =
    sb ++= serialize(dp)

  private def serialize(dataPoint: DataPoint): String =
    Array(
      dataPoint.id.toString,
      dataPoint.upstreamId.toString,
      dataPoint.queryId.toString,
      dataPoint.schedulingThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.scheduledTime).toString,
      dataPoint.executionThreadId.toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.startTime).toString,
      TimeUnit.NANOSECONDS.toMicros(dataPoint.stopTime).toString,
      dataPoint.task.toString
    ).mkString(SEPARATOR) + "\n"
}

object CsvStdOutDataWriter {
  private val SEPARATOR = ","
  private val HEADER = Array("id",
                             "upstreamId",
                             "queryId",
                             "schedulingThreadId",
                             "schedulingTime(us)",
                             "executionThreadId",
                             "startTime(us)",
                             "stopTime(us)",
                             "pipeline").mkString(SEPARATOR) + "\n"
}
