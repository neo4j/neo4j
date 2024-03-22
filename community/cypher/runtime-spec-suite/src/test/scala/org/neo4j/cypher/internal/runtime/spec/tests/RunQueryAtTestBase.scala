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

import org.mockito.Mockito
import org.neo4j.configuration.helpers.RemoteUri
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.kernel.impl.query.DelegatingTransactionalContext
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TestQueryExecution
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.InternalLogProvider
import org.neo4j.util.AnyValueConversions
import org.neo4j.util.Table
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import java.util.UUID

abstract class RunQueryAtTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) with AnyValueConversions {

  private var stubbedExecution: Function[(String, MapValue), TestQueryExecution] = args => {
    TestQueryExecution.fromThrowable(
      Seq.empty,
      new IllegalStateException(s"No matching stub for ${args._1}")
    )
  }

  private def stubExecution[A](f: Function[(String, MapValue), TestQueryExecution])(block: => A): A = {
    val prev = stubbedExecution
    try {
      stubbedExecution = f
      block
    } finally {
      stubbedExecution = prev
    }
  }

  private def stubExecutionTable[A](table: Table)(block: => A): A = {
    stubExecution((_: (String, MapValue)) => table)(block)
  }

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, runtime, workloadMode, logProvider, debugOptions) {
      override protected def wrapTransactionContext(ctx: TransactionalContext): TransactionalContext =
        new DelegatingTransactionalContext(ctx) {
          override def constituentTransactionFactory: ConstituentTransactionFactory =
            new ConstituentTransactionFactory {
              def transactionFor(databaseReference: DatabaseReference)
                : ConstituentTransactionFactory.ConstituentTransaction =
                (query: String, parameters: MapValue, querySubscriber: QuerySubscriber) => {
                  val ex = stubbedExecution.apply(query, parameters)
                  ex.subscriber = querySubscriber
                  ex
                }

              def sessionDatabase(): DatabaseReferenceImpl.Composite =
                new DatabaseReferenceImpl.Composite(
                  new NormalizedDatabaseName("composite"),
                  DatabaseIdFactory.from("composite", UUID.randomUUID()),
                  java.util.Set.of(
                    internalConstituent("local", UUID.randomUUID()),
                    remoteConstituent("remote", UUID.randomUUID())
                  )
                )
            }
        }
    }
  }

  private def internalConstituent(name: String, id: UUID) =
    new DatabaseReferenceImpl.Internal(
      new NormalizedDatabaseName(name),
      new NormalizedDatabaseName("composite"),
      DatabaseIdFactory.from("name", id),
      false
    )

  private def remoteConstituent(alias: String, id: UUID) =
    new DatabaseReferenceImpl.External(
      new NormalizedDatabaseName("remoteDb"),
      new NormalizedDatabaseName(alias),
      new NormalizedDatabaseName("composite"),
      Mockito.mock(classOf[RemoteUri]),
      id
    )

  test("should forward results from a remote constituent") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.remote", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    stubExecutionTable(Table.hdr("n").row(1)) {
      val result = execute(plan, runtime)
      result should beColumns("n").withRows(Seq(Array(1)))
    }
  }

  test("should forward results from a local constituent") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.local", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    stubExecutionTable(Table.hdr("n").row(1)) {
      val result = execute(plan, runtime)
      result should beColumns("n").withRows(Seq(Array(1)))
    }
  }

  test("should support querying the graph by name") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "graph.byName('composite.remote')", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    stubExecutionTable(Table.hdr("n").row(1)) {
      val result = execute(plan, runtime)
      result should beColumns("n").withRows(Seq(Array(1)))
    }
  }

  test("should support querying the graph by name dynamically") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "graph.byName(g)", Set.empty, Map.empty, Set("n"))
      .projection("'composite.remote' as g")
      .argument()
      .build()

    stubExecutionTable(Table.hdr("n").row(1)) {
      val result = execute(plan, runtime)
      result should beColumns("n").withRows(Seq(Array(1)))
    }
  }

  test("should forward query to the constituent") {
    val query = "MATCH (n) RETURN n"

    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt(query, "composite.remote", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    stubExecution { case (q, _) =>
      q shouldBe query

      Table.hdr("n")
    } {
      val result = execute(plan, runtime)
      result.awaitAll()
    }
  }

  test("should forward local parameters to the constituent") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.remote", Set.empty, Map("param" -> "p"), Set("n"))
      .projection("'value' as p")
      .argument()
      .build()

    stubExecution { case (_, p) =>
      p should have size 1
      p.get("param") shouldBe Values.stringValue("value")

      Table.hdr("n")
    } {
      val result = execute(plan, runtime)
      result.awaitAll()
    }
  }

  test("should forward global parameters to the constituent") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.remote", Set("param"), Map.empty, Set("n"))
      .argument()
      .build()

    stubExecution { case (_, p) =>
      p should have size 1
      p.get("param") shouldBe Values.stringValue("value")

      Table.hdr("n")
    } {
      val result = execute(plan, runtime, Map("param" -> "value"))
      result.awaitAll()
    }
  }

  test("should throw an exception if the constituent is not found") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.invalid", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    val result = execute(plan, runtime)

    an[EntityNotFoundException] should be thrownBy result.awaitAll()
  }

  test("should throw an exception if the headers do not match") {
    val plan = new LogicalQueryBuilder(this)
      .produceResults("n")
      .runQueryAt("MATCH (n) RETURN n", "composite.remote", Set.empty, Map.empty, Set("n"))
      .argument()
      .build()

    stubExecutionTable(Table.hdr("m").row(1)) {
      val result = execute(plan, runtime)

      an[AssertionError] should be thrownBy result.awaitAll()
    }
  }

}
