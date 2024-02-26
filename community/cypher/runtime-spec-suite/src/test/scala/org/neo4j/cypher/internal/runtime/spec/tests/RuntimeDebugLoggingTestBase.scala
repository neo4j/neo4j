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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.RuntimeDebugLoggingTestBase.withDebugLog
import org.neo4j.logging.LogAssert

abstract class RuntimeDebugLoggingTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](withDebugLog(edition), runtime)
    with RandomValuesTestSupport {

  test("log ignored errors in transaction foreach") {
    assume(runtime.name != "interpreted")
    val status = if (random.nextBoolean()) Some("status") else None
    val size = random.nextInt(sizeHint) + 1
    val failureAt = random.nextInt(size)
    val errorBehaviour = randomAmong(Seq(OnErrorContinue, OnErrorBreak))

    // then
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionForeach(1, onErrorBehaviour = errorBehaviour, maybeReportAs = status)
      .|.projection(s"1 / (x - $failureAt) as maybeBang")
      .|.argument()
      .unwind(s"range(0, ${size - 1}) as x")
      .argument()
      .build()

    val result = execute(buildPlan(logicalQuery, runtime), readOnly = true, implicitTx = true)

    val expected = Range(0, size).map(x => Array[Any](x))
    result should beColumns("x").withRows(inOrder(expected))

    val logAssert = new LogAssert(logProvider)
    logAssert.containsMessages("Recover error in inner transaction")
  }

  test("log ignored errors in transaction apply") {
    val status = if (random.nextBoolean()) Some("status") else None
    val size = random.nextInt(sizeHint) + 1
    val failureAt = random.nextInt(size)
    val errorBehaviour = randomAmong(Seq(OnErrorContinue, OnErrorBreak))

    // then
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .transactionApply(1, onErrorBehaviour = errorBehaviour, maybeReportAs = status)
      .|.projection(s"1 / (x - $failureAt) as maybeBang")
      .|.argument()
      .unwind(s"range(0, ${size - 1}) as x")
      .argument()
      .build()

    val result = execute(buildPlan(logicalQuery, runtime), readOnly = true, implicitTx = true)

    val expected = Range(0, size).map(x => Array[Any](x))
    result should beColumns("x").withRows(inOrder(expected))

    val logAssert = new LogAssert(logProvider)
    logAssert.containsMessages("Recover error in inner transaction")
  }
}

object RuntimeDebugLoggingTestBase {

  def withDebugLog[C <: RuntimeContext](edition: Edition[C]): Edition[C] = edition.copyWith(
    GraphDatabaseSettings.debug_log_enabled -> java.lang.Boolean.TRUE
  )
}
