/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.cypher._
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.graphdb.{QueryExecutionException, TransactionFailureException}
import org.neo4j.internal.cypher.acceptance.TestResourceProcedure._
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.impl.proc.Procedures

import scala.collection.mutable.ArrayBuffer

class ReflectiveProcedureCallAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport {

  def query(resultCount: Long, failCount: Long) =
    s"""
      |CALL org.neo4j.test.testOnCloseFailingResourceProcedure($resultCount) YIELD value as v1
      |WITH v1
      |CALL org.neo4j.test.testResourceProcedure($resultCount) YIELD value as v2
      |WITH v1, v2
      |CALL org.neo4j.test.testOnCloseFailingResourceProcedure($resultCount) YIELD value as v3
      |WITH v1, v2, v3
      |CALL org.neo4j.test.testFailingResourceProcedure($failCount) YIELD value as v4
      |WITH v1, v2, v3, v4
      |CALL org.neo4j.test.testResourceProcedure($resultCount) YIELD value as v5
      |RETURN v1, v2, v3, v4, v5
    """.stripMargin

  val defaultQuery = query(resultCount = 4, failCount = 3)

  private def setUpProcedures(): TestResourceProcedure.Counters = {
    val procedures = graph.getDependencyResolver.resolveDependency(classOf[Procedures])
    val counters = new TestResourceProcedure.Counters
    procedures.registerComponent(classOf[TestResourceProcedure.Counters], TestResourceProcedure.countersProvider(counters), true)

    procedures.registerProcedure(classOf[TestResourceProcedure])
    procedures.registerFunction(classOf[TestResourceProcedure])

    counters
  }

  test("should close resources on failure") {
    val counters = setUpProcedures()

    val caught = intercept[QueryExecutionException] {
      val result = graph.execute(defaultQuery)
      result.resultAsString()
    }
    val rootCause = ExceptionUtils.getRootCause(caught)
    rootCause shouldBe a[SimulateFailureException]

    val suppressed = collectSuppressed(caught)
    suppressed.head shouldBe a[CypherExecutionException]
    ExceptionUtils.getRootCause(suppressed.head) shouldBe a[SimulateFailureOnCloseException]

    counters.liveCountTestResourceProcedure() shouldEqual 0
    counters.liveCountTestFailingResourceProcedure() shouldEqual 0
    counters.liveCountTestOnCloseFailingResourceProcedure() shouldEqual 0

    counters.closeCountTestResourceProcedure shouldEqual 3 // 1 close from exhausting into v5 + 2 closes on failure
    counters.closeCountTestFailingResourceProcedure shouldEqual 1
    counters.closeCountTestOnCloseFailingResourceProcedure shouldEqual 2
  }

  test("should close resources on mid-stream transaction close") {
    val counters = setUpProcedures()

    val tx = graph.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    val result = graph.execute(defaultQuery)

    // Pull one row and then close the transaction
    result.next()

    // Close transaction while the result is still open
    val caught = intercept[TransactionFailureException] {
      tx.close()
    }
    val rootCause = ExceptionUtils.getRootCause(caught)
    rootCause shouldBe a[SimulateFailureOnCloseException]

    val suppressed = collectSuppressed(caught)
    suppressed.head shouldBe a[SimulateFailureOnCloseException]

    counters.liveCountTestResourceProcedure() shouldEqual 0
    counters.liveCountTestFailingResourceProcedure() shouldEqual 0
    counters.liveCountTestOnCloseFailingResourceProcedure() shouldEqual 0

    counters.closeCountTestResourceProcedure shouldEqual 2
    counters.closeCountTestFailingResourceProcedure shouldEqual 1
    counters.closeCountTestOnCloseFailingResourceProcedure shouldEqual 2
  }

  test("should not leave any resources open on transaction close before pulling on the result") {
    val counters = setUpProcedures()

    val tx = graph.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    val result = graph.execute(defaultQuery)

    // Close the transaction directly without pulling the result
    tx.close()

    counters.liveCountTestResourceProcedure() shouldEqual 0
    counters.liveCountTestFailingResourceProcedure() shouldEqual 0
    counters.liveCountTestOnCloseFailingResourceProcedure() shouldEqual 0
  }

  test("should handle tracking many closeable resources without stockpiling them until the end of the transaction") {
    val counters = setUpProcedures()
    val numberOfRows = 100

    val tx = graph.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    val result = graph.execute(s"UNWIND range(1,$numberOfRows) as i CALL org.neo4j.test.testResourceProcedure(1) YIELD value RETURN value")

    // Pull one row and then close the transaction
    var i = 0
    while (i < numberOfRows - 1 && result.hasNext) {
      result.next()
      i += 1
    }
    val counterBeforeTxClose = counters.closeCountTestResourceProcedure
    tx.close()
    val counterAfterTxClose = counters.closeCountTestResourceProcedure

    counters.liveCountTestResourceProcedure() shouldEqual 0
    counters.closeCountTestResourceProcedure shouldEqual numberOfRows
    (counterAfterTxClose - counterBeforeTxClose) shouldEqual 1 // Only the last open stream should need to be closed at transaction closure
  }

  test("should close resources on failure with periodic commit") {
    val counters = setUpProcedures()

    val url = createTempCSVFile(10)
    val periodicCommitQuery =
      s"""USING PERIODIC COMMIT 3
         |LOAD CSV FROM '$url' AS line
         |CREATE ()
         |WITH line
         |CALL org.neo4j.test.testResourceProcedure(4) YIELD value as v1
         |WITH line, v1,
         |  CASE line
         |    WHEN ['8'] THEN org.neo4j.test.fail("8")
         |    ELSE 'ok'
         |  END as ok
         |RETURN line, v1, ok
         |""".stripMargin

    val caught = intercept[QueryExecutionException] {
      val result = graph.execute(periodicCommitQuery)
      result.resultAsString()
    }
    val rootCause = ExceptionUtils.getRootCause(caught)
    rootCause shouldBe a[SimulateFailureException]

    counters.liveCountTestResourceProcedure() shouldEqual 0
    counters.liveCountTestFailingResourceProcedure() shouldEqual 0
    counters.liveCountTestOnCloseFailingResourceProcedure() shouldEqual 0

    counters.closeCountTestResourceProcedure shouldEqual 8
    counters.closeCountTestFailingResourceProcedure shouldEqual 0 // Unused
    counters.closeCountTestOnCloseFailingResourceProcedure shouldEqual 0 // Unused
  }

  private def collectSuppressed(t: Throwable): Seq[Throwable] = {
    val suppressed = ArrayBuffer[Throwable](t.getSuppressed:_*)
    var cause = t.getCause
    while (cause != null) {
      suppressed ++= cause.getSuppressed
      val nextCause = cause.getCause
      cause = if (nextCause == null || cause == nextCause) null else nextCause
    }
    suppressed
  }

  def createTempCSVFile(numberOfLines: Int): String =
    createTempFileURL("file", ".csv") { writer: PrintWriter =>
      1.to(numberOfLines).foreach { n: Int => writer.println(n.toString) }
    }

}
