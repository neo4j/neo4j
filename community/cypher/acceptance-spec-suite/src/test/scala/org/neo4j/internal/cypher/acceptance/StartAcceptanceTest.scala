/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class StartAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("START n=node:index(key = \"value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index(key = "value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node:index(\"key:value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index("key:value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("start + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
      graph.index.forNodes("index").add(otherNode, "key", "value")
    }

    val result = executeWithAllPlannersAndCompatibilityMode("""START n=node:index("key:value") WHERE n.prop = 42 RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("Relationship legacy index") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index.forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(Map("r"-> relationship)))
  }

  test("Relationship legacy index mk II") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index.forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') MATCH (a)-[r]-(b) RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(
      Map("r"-> relationship),
      Map("r"-> relationship)
    ))
  }

  test("Relationship legacy index mk III") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index.forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "START r=relationship:relIndex('key:*') MATCH (a)-[r]->(b) RETURN r"
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(
      Map("r"-> relationship)
    ))
  }
}
