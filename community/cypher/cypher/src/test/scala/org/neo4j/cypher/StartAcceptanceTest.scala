/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

class StartAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("START r=rel(0) RETURN r") {
    val rel = relate(createNode(), createNode())
    val result = execute("START r=rel(0) RETURN r").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START r=rel:index(key = \"value\") RETURN r") {
    val rel = relate(createNode(), createNode())
    graph.inTx {
      graph.index.forRelationships("index").add(rel, "key", "value")
    }

    val result = execute("""START r=rel:index(key = "value") RETURN r""").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START n=node(0) RETURN n") {
    val node = createNode()
    val result = execute("START n=node(0) RETURN n").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node:index(key = \"value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithNewPlanner("""START n=node:index(key = "value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START r=rel:index(\"key:value\") RETURN r") {
    val rel = relate(createNode(), createNode())
    graph.inTx {
      graph.index.forRelationships("index").add(rel, "key", "value")
    }

    val result = execute("""START r=rel:index("key:value") RETURN r""").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START n=node:index(\"key:value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithNewPlanner("""START n=node:index("key:value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }
}
