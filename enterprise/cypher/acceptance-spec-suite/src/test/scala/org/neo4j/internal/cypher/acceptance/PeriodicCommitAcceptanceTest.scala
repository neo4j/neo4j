/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.neo4j.cypher._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{PageCacheHits, PageCacheMisses, PlannerImpl}
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore

class PeriodicCommitAcceptanceTest extends ExecutionEngineFunSuite
  with TxCountsTrackingTestSupport with QueryStatisticsTestSupport
  with CreateTempFileTestSupport {

  def unwrapLoadCSVStatus[T](f: => T) = {
    try {
      f
    }
    catch {
      case e: LoadCsvStatusWrapCypherException => throw e.getCause
    }
  }

  def createTempCSVFile(numberOfLines: Int): String =
    createTempFileURL("file", ".csv") { writer: PrintWriter =>
      1.to(numberOfLines).foreach { n: Int => writer.println(n.toString) }
    }

  private def createFile(f: PrintWriter => Unit) = createTempFileURL("cypher", ".csv")(f).cypherEscape

  test("should reject periodic commit when not followed by LOAD CSV") {
    evaluating {
      executeScalar("USING PERIODIC COMMIT 200 MATCH (n) RETURN count(n)")
    } should produce[SyntaxException]
  }

  test("should produce data from periodic commit") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("42")
    }
    val result = execute(s"USING PERIODIC COMMIT 200 LOAD CSV FROM '$url' AS line CREATE (n {id: line[0]}) RETURN n.id")

    result.toList should equal(List(Map("n.id" -> "42")))
    result.columns should equal(List("n.id"))
  }

  test("should use cost planner for periodic commit and load csv") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
      writer.println("5")
    }

    // to make sure the property key id is created before the tx in order to not mess up with the tx counts
    createNode(Map("id" -> 42))

    val txIdStore = graph.getDependencyResolver.resolveDependency(classOf[TransactionIdStore])
    val beforeTxId = txIdStore.getLastClosedTransactionId
    val result = execute(s"PROFILE USING PERIODIC COMMIT 1 LOAD CSV FROM '$url' AS line CREATE (n {id: line[0]}) RETURN n.id as id")
    val arguments = result.executionPlanDescription().arguments
    arguments should contain(PlannerImpl("IDP"))
    arguments.find( _.isInstanceOf[PageCacheHits]) shouldBe defined
    arguments.find( _.isInstanceOf[PageCacheMisses]) shouldBe defined
    result.columnAs[Long]("id").toList should equal(List("1","2","3","4","5"))
    val afterTxId = txIdStore.getLastClosedTransactionId
    result.close()

    afterTxId should equal(beforeTxId + 5)
  }

  test("should support simple periodic commit") {
    // given
    val url = createTempCSVFile(5)
    val queryText =
      "USING PERIODIC COMMIT 2 " +
      s"LOAD CSV FROM '$url' AS line " +
      "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 5)

    // and then
    txCounts should equal(TxCounts(commits = 3))
  }

  test("should support simple periodic commit with unaligned batch size") {
    // given
    val url = createTempCSVFile(4)
    val queryText =
      "USING PERIODIC COMMIT 3 " +
      s"LOAD CSV FROM '$url' AS line " +
      "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 4)

    // and then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should abort first tx when failing on first batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: (toInt(line[0]) - 8)/0})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](
      unwrapLoadCSVStatus(executeScalar[Number](queryText))
    ))

    // then
    txCounts should equal(TxCounts(rollbacks = 1))
  }

  test("should not mistakenly use closed statements") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line MERGE (:Label);"

    // when
    val (_, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 2, rollbacks = 0))
  }

  test("should commit first tx and abort second tx when failing on second batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: 1 / (toInt(line[0]) - 16)})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](
      unwrapLoadCSVStatus(executeScalar[Number](queryText))
    ))

    // then
    txCounts should equal(TxCounts(commits = 1, rollbacks = 1))
  }

  test("should support periodic commit hint without explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
  }

  test("should support periodic commit hint with explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT 400 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
  }

  test("should reject periodic commit hint with negative size") {
    val url = createTempCSVFile(1)
    intercept[SyntaxException] {
      executeScalar[Node](s"USING PERIODIC COMMIT -1 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
    }
  }

  test("should fail if periodic commit is executed in an open transaction") {
    // given
    intercept[PeriodicCommitInOpenTransactionException] {
      val url = createTempCSVFile(3)
      graph.inTx( {
        execute(s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE ()")
      }, Type.explicit)
    }
  }

  test("should tell line number information when failing using periodic commit and load csv") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
      writer.println("0")
      writer.println("3")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '$url' AS line " +
        s"CREATE ({name: 1/toInt(line[0])})"

    // when executing 5 updates
    val e = intercept[CypherException](execute(queryText))

    // then
    e.getMessage should include("on line 3. Possibly the last row committed during import is line 2. Note that this information might not be accurate.")
  }

  test("should read committed properties in later transactions") {

    val csvFile = """name,flows,kbytes
                    |a,1,10.0
                    |b,1,10.0
                    |c,1,10.0
                    |d,1,10.0
                    |e,1,10.0
                    |f,1,10.0""".stripMargin

    val url = createFile(writer => writer.print(csvFile))

    val query = s"""CYPHER runtime=interpreted USING PERIODIC COMMIT 2 LOAD CSV WITH HEADERS FROM '$url' AS row
                  |MERGE (n {name: row.name})
                  |ON CREATE SET
                  |n.flows = toInt(row.flows),
                  |n.kbytes = toFloat(row.kbytes)
                  |
                  |ON MATCH SET
                  |n.flows = n.flows + toInt(row.flows),
                  |n.kbytes = n.kbytes + toFloat(row.kbytes)""".stripMargin

    def nodesWithFlow(i: Int) = s"MATCH (n {flows: $i}) RETURN count(*)"

    // Given
    execute(query)
    executeScalar[Int](nodesWithFlow(1)) should be(6)

    // When
    execute(query)

    // Then
    executeScalar[Int](nodesWithFlow(2)) should be(6)
  }
}
