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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.nodeValue
import org.neo4j.values.virtual.VirtualValues.relationshipValue

import java.util.Collections

abstract class InputWithMaterializedEntitiesTest[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite(edition, runtime) {

  test("node property access") {
    val node = givenGraph { createNode(1, "Person", Map("name" -> "Anna")) }
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
    val relationship =
      givenGraph { createRelationship(1, createNode(1), createNode(2), "AWESOME_RELATIONSHIP", Map("active" -> true)) }
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
    val (node1, node2) = givenGraph {
      val node1 = createNode(1, "Person", Map("name" -> "Anna"))
      val node2 = createNode(2)
      (node1, node2)
    }
    val input = inputValues(Array(node1, node2))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("name")
      .projection("n.name AS name")
      .filter("n.name IS NOT NULL")
      .input(variables = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("name").withSingleRow("Anna")
  }

  test("relationship property existence") {
    val (relationship1, relationship2) = givenGraph {
      val node1 = createNode(1)
      val node2 = createNode(2)
      val relationship1 = createRelationship(1, node1, node2, "AWESOME_RELATIONSHIP", Map("active" -> true))
      val relationship2 = createRelationship(2, node1, node2, "AWESOME_RELATIONSHIP", Map())
      (relationship1, relationship2)
    }
    val input = inputValues(Array(relationship1, relationship2))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("active")
      .projection("r.active AS active")
      .filter("r.active IS NOT NULL")
      .input(variables = Seq("r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("active").withSingleRow(true)
  }

  test("label existence check") {
    val node = givenGraph { createNode(1, "Person", Map("name" -> "Anna")) }
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
    val node = givenGraph { createNode(1, "Person", Map("name" -> "Anna")) }
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
    val (startNode, endNode) = givenGraph {
      val startNode = createNode(1)
      val endNode = createNode(2)
      (startNode, endNode)
    }

    val relationship = createRelationship(1, startNode, endNode, "AWESOME_RELATIONSHIP", Map("active" -> true))
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
    val node = givenGraph { createNode(1, "Person", Map("name" -> "Anna")) }
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
    val node = givenGraph { createNode(123, "Person", Map("name" -> "Anna")) }
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
    val (startNode, endNode) = givenGraph {
      val startNode = nodeValue(1, "1", Values.stringArray(), MapValue.EMPTY)
      val endNode = nodeValue(2, "2", Values.stringArray(), MapValue.EMPTY)
      (startNode, endNode)
    }

    val relationship = createRelationship(1, startNode, endNode, "AWESOME_RELATIONSHIP", Map("active" -> true))
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

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, runtime, workloadMode, logProvider) {

      override protected def newRuntimeContext(queryContext: QueryContext): CONTEXT = {
        runtimeContextManager.create(
          queryContext,
          queryContext.transactionalContext.schemaRead,
          queryContext.transactionalContext.procedures,
          MasterCompiler.CLOCK,
          CypherDebugOptions.default,
          compileExpressions = false,
          materializedEntitiesMode = true,
          operatorEngine = CypherOperatorEngineOption.default,
          interpretedPipesFallback = CypherInterpretedPipesFallbackOption.default,
          anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
          () => {}
        )
      }
    }
  }

  private def createNode(id: Long, label: String, properties: Map[String, Any]): NodeValue = {
    val labelValue =
      if (label == null) {
        Values.stringArray()
      } else {
        Values.stringArray(label)
      }

    nodeValue(id, "n", labelValue, convertProperties(properties))
  }

  private def createNode(id: Long): NodeValue = {
    createNode(id, null, Map())
  }

  private def createRelationship(
    id: Long,
    startNode: NodeValue,
    endNode: NodeValue,
    relType: String,
    properties: Map[String, Any]
  ): RelationshipValue = {
    relationshipValue(id, "r", startNode, endNode, Values.stringValue(relType), convertProperties(properties))
  }

  private def convertProperties(properties: Map[String, Any]): MapValue = {
    VirtualValues.map(properties.keys.toArray, properties.values.map(v => Values.of(v)).toArray)
  }
}
