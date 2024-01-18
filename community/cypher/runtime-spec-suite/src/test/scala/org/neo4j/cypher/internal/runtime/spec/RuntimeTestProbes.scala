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

import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.values.AnyValue
import org.scalatest.Assertion

import java.io.PrintStream
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

/**
 * A collection of runtime test probes to be used with the [[Prober]] logical plan / operator.
 */
object RuntimeTestProbes

/**
 * Test probe that will apply an assertion on the delta of the query statistics between rows.
 */
class QueryStatisticsProbe(assertion: QueryStatistics => Assertion)(runtimeTestUtils: RuntimeTestUtils)(
  chain: Prober.Probe = null
) extends Prober.Probe {
  private[this] var _prevTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty
  private[this] var _thisTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty
  private[this] var _transactionsCommitted = 0

  override def onRow(row: AnyRef, state: AnyRef): Unit = {
    val statistics = runtimeTestUtils.queryStatistics(state)
    val transactionsCommitted = statistics.getTransactionsCommitted
    if (_transactionsCommitted != transactionsCommitted) {
      assertion(_thisTxQueryStatistics.-(_prevTxQueryStatistics))
      _transactionsCommitted = transactionsCommitted
      _prevTxQueryStatistics = org.neo4j.cypher.internal.runtime.QueryStatistics.empty.+(_thisTxQueryStatistics)
    }
    _thisTxQueryStatistics = statistics

    if (chain != null) {
      chain.onRow(row, state)
    }
  }
}

object QueryStatisticsProbe {

  def apply(assertion: QueryStatistics => Assertion, runtimeTestUtils: RuntimeTestUtils): QueryStatisticsProbe =
    new QueryStatisticsProbe(assertion)(runtimeTestUtils)()
}

/**
 * Probe that records the requested variables for each row that it sees
 * Inject into the query using a [[Prober]] plan
 */
class RecordingProbe(variablesToRecord: String*)(chain: Prober.Probe = null) extends Prober.Probe()
    with RecordingRowsProbe {
  private[this] val _seenRows = new ArrayBuffer[Array[AnyValue]]

  override def onRow(row: AnyRef, state: AnyRef): Unit = {
    val cypherRow = row.asInstanceOf[CypherRow]
    val recordedVars = variablesToRecord.toArray.map { v =>
      cypherRow.getByName(v)
    }
    _seenRows += recordedVars

    if (chain != null) {
      chain.onRow(row, state)
    }
  }

  override def seenRows: Array[Array[AnyValue]] = {
    _seenRows.toArray
  }
}

object RecordingProbe {

  def apply(variablesToRecord: String*): RecordingProbe =
    new RecordingProbe(variablesToRecord: _*)()
}

trait RecordingRowsProbe {
  def seenRows: Array[Array[AnyValue]]
}

/**
 * Thread-safe probe that records the requested variables for each row that it sees
 * Inject into the query using a [[Prober]] plan
 */
class ThreadSafeRecordingProbe(variablesToRecord: String*)(chain: Prober.Probe = null) extends Prober.Probe()
    with RecordingRowsProbe {
  private[this] val _seenRows = new ConcurrentLinkedQueue[Array[AnyValue]]

  override def onRow(row: AnyRef, state: AnyRef): Unit = {
    val cypherRow = row.asInstanceOf[CypherRow]
    val recordedVars = variablesToRecord.toArray.map { v =>
      cypherRow.getByName(v)
    }
    _seenRows.add(recordedVars)

    if (chain != null) {
      chain.onRow(row, state)
    }
  }

  override def seenRows: Array[Array[AnyValue]] = {
    _seenRows.toArray().map(_.asInstanceOf[Array[AnyValue]])
  }
}

object ThreadSafeRecordingProbe {

  def apply(variablesToRecord: String*): ThreadSafeRecordingProbe =
    new ThreadSafeRecordingProbe(variablesToRecord: _*)()
}

/**
 * Probe that prints the requested variables for each row that it sees
 * Inject into the query using a [[Prober]] plan
 */
class PrintingProbe(variablesToPrint: String*)(
  prefix: String = "",
  override val name: String = "",
  printStream: PrintStream = System.out
)(chain: Prober.Probe = null) extends Prober.Probe() {
  private[this] var rowCount = 0L

  override def onRow(row: AnyRef, state: AnyRef): Unit = {
    val cypherRow = row.asInstanceOf[CypherRow]
    val variablesString = variablesToPrint.toArray.map { v =>
      val value = cypherRow.getByName(v)
      s"$v = $value"
    }.mkString("{ ", ", ", " }")
    printStream.println(s"$prefix Row: $rowCount $variablesString")
    rowCount += 1

    if (chain != null) {
      chain.onRow(row, state)
    }
  }
}

object PrintingProbe {

  def apply(variablesToRecord: String*): PrintingProbe =
    new PrintingProbe(variablesToRecord: _*)()()
}

object RecordingAndPrintingProbe {

  def apply(variablesToRecord: String*): RecordingProbe =
    new RecordingProbe(variablesToRecord: _*)(new PrintingProbe(variablesToRecord: _*)()())
}
