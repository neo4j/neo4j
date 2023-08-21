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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig

case class RuntimeTestParameters(
  sleepSubscriber: Option[SleepPerNRows] = None, // Sleep nanoseconds per result row
  busySubscriber: Boolean = false,
  killTransactionAfterRows: Option[Long] = None,
  resultConsumptionController: RuntimeTestResultConsumptionController = ConsumeAllThenCloseResultConsumer,
  printProgress: Option[PrintEveryNRows] = None, // Print record count every n rows, e.g. Some(PrintEveryNRows(1000))
  printConfig: Boolean = false,
  planCombinationRewriter: Option[TestPlanCombinationRewriterConfig] = None
)

case class SleepPerNRows(sleepNanos: Int, perNRows: Int)

case class PrintEveryNRows(
  everyNRows: Long,
  messagePrefix: String = "Result row #",
  printRowCount: Boolean = true,
  messageSuffix: String = System.lineSeparator()
)
