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
package org.neo4j.cypher.internal.runtime.spec.tests

import java.util.Collections

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.{CypherRuntime, MasterCompiler, RuntimeContext}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValue, VirtualValues}

abstract class InputWithNoDbAccessTest[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT]) extends RuntimeTestSuite(edition, runtime) {
  
  test("node property access") {
    val label = Values.stringArray("Person")
    val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
    val node = VirtualValues.nodeValue(1, label, properties)
    val input = inputValues(Array(node))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name")
      .projection("n.name AS name")
      .input(variables = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("name").withSingleRow("Anna")
  }

  test("relationship property access") {
    val node1 = VirtualValues.nodeValue(1, Values.stringArray(), MapValue.EMPTY)
    val node2 = VirtualValues.nodeValue(2, Values.stringArray(), MapValue.EMPTY)

    val relType = Values.stringValue("AWESOME_RELATIONSHIP")
    val properties = VirtualValues.map(Array("active"), Array(Values.booleanValue(true)))
    val relationship = VirtualValues.relationshipValue(1, node1, node2, relType, properties)
    val input = inputValues(Array(relationship))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("active")
      .projection("r.active AS active")
      .input(variables = Seq("r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("active").withSingleRow(true)
  }

  test("node property existence") {
    val label = Values.stringArray("Person")
    val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
    val node1 = VirtualValues.nodeValue(1, label, properties)
    val node2 = VirtualValues.nodeValue(2, label, MapValue.EMPTY)
    val input = inputValues(Array(node1, node2))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name")
      .projection("n.name AS name")
      .filter("EXISTS (n.name)")
      .input(variables = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("name").withSingleRow("Anna")
  }

  test("relationship property existence") {
    val node1 = VirtualValues.nodeValue(1, Values.stringArray(), MapValue.EMPTY)
    val node2 = VirtualValues.nodeValue(2, Values.stringArray(), MapValue.EMPTY)

    val relType = Values.stringValue("AWESOME_RELATIONSHIP")
    val properties = VirtualValues.map(Array("active"), Array(Values.booleanValue(true)))
    val relationship1 = VirtualValues.relationshipValue(1, node1, node2, relType, properties)
    val relationship2 = VirtualValues.relationshipValue(2, node1, node2, relType, MapValue.EMPTY)
    val input = inputValues(Array(relationship1, relationship2))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("active")
      .projection("r.active AS active")
      .filter("EXISTS (r.active)")
      .input(variables = Seq("r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("active").withSingleRow(true)
  }

  test("label existence check") {
    val label = Values.stringArray("Person")
    val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
    val node = VirtualValues.nodeValue(1, label, properties)
    val input = inputValues(Array(node))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name")
      .projection("n.name AS name")
      .filter("n:Person")
      .input(variables = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("name").withSingleRow("Anna")
  }

  test("node 'keys' function") {
    val label = Values.stringArray("Person")
    val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
    val node = VirtualValues.nodeValue(1, label, properties)
    val input = inputValues(Array(node))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keys")
      .projection("keys(n) AS keys")
      .input(variables = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("keys").withSingleRow(Collections.singletonList("name"))
  }

  test("relationship 'keys' function") {
        val node1 = VirtualValues.nodeValue(1, Values.stringArray(), MapValue.EMPTY)
        val node2 = VirtualValues.nodeValue(2, Values.stringArray(), MapValue.EMPTY)

        val relType = Values.stringValue("AWESOME_RELATIONSHIP")
        val properties = VirtualValues.map(Array("active"), Array(Values.booleanValue(true)))
        val relationship = VirtualValues.relationshipValue(1, node1, node2, relType, properties)
        val input = inputValues(Array(relationship))


    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keys")
      .projection("keys(r) AS keys")
      .input(variables = Seq("r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("keys").withSingleRow(Collections.singletonList("active"))
  }

    test("node 'labels' function") {
      val label = Values.stringArray("Person")
      val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
      val node = VirtualValues.nodeValue(1, label, properties)
      val input = inputValues(Array(node))

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("labels")
        .projection("labels(n) AS labels")
        .input(variables = Seq("n"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      runtimeResult should beColumns("labels").withSingleRow(Collections.singletonList("Person"))
    }

      test("node id") {
        val label = Values.stringArray("Person")
        val properties = VirtualValues.map(Array("name"), Array(Values.stringValue("Anna")))
        val node = VirtualValues.nodeValue(123, label, properties)
        val input = inputValues(Array(node))

        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("id(n) AS id")
          .input(variables = Seq("n"))
          .build()

        val runtimeResult = execute(logicalQuery, runtime, input)

        // then
        runtimeResult should beColumns("id").withSingleRow(123)
      }

    test("relationship 'type' function") {
          val node1 = VirtualValues.nodeValue(1, Values.stringArray(), MapValue.EMPTY)
          val node2 = VirtualValues.nodeValue(2, Values.stringArray(), MapValue.EMPTY)

          val relType = Values.stringValue("AWESOME_RELATIONSHIP")
          val properties = VirtualValues.map(Array("active"), Array(Values.booleanValue(true)))
          val relationship = VirtualValues.relationshipValue(1, node1, node2, relType, properties)
          val input = inputValues(Array(relationship))


      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("type")
        .projection("type(r) AS type")
        .input(variables = Seq("r"))
        .build()

      val runtimeResult = execute(logicalQuery, runtime, input)

      // then
      runtimeResult should beColumns("type").withSingleRow("AWESOME_RELATIONSHIP")
    }

  override protected def createRuntimeTestSupport(graphDb: GraphDatabaseService, edition: Edition[CONTEXT], workloadMode: Boolean): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, workloadMode) {

      override protected def newRuntimeContext(txContext: TransactionalContext, queryContext: QueryContext): CONTEXT = {
        runtimeContextManager.create(queryContext,
          txContext.kernelTransaction().schemaRead(),
          MasterCompiler.CLOCK,
          Set.empty,
          compileExpressions = false,
          noDatabaseAccess = true)
      }
    }
  }
}
