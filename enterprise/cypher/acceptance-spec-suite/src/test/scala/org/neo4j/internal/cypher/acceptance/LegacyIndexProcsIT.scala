/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite}

class LegacyIndexProcsIT extends ExecutionEngineFunSuite {

  test("Node from exact key value match") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute( """CALL db.nodeLegacyIndexSeek('index', 'key', 'value') YIELD node AS n RETURN n""").toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node seek") {
    a [CypherExecutionException] should be thrownBy
      execute("""CALL db.nodeLegacyIndexSeek('index', 'key', 'value') YIELD node AS n RETURN n""")
  }

  test("Node from query lucene index") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute( """CALL db.nodeLegacyIndexSearch("index", "key:value") YIELD node as n RETURN n""").toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node search") {
    a [CypherExecutionException] should be thrownBy
      execute("""CALL db.nodeLegacyIndexSearch('index', 'key:value') YIELD node AS n RETURN n""")
  }

  test("legacy index + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
      graph.index().forNodes("index").add(otherNode, "key", "value")
    }

    val result = execute(
      """CALL db.nodeLegacyIndexSearch("index", "key:value") YIELD node AS n WHERE n.prop = 42 RETURN n""").toList

    result should equal(List(Map("n" -> node)))
  }

  test("Relationship legacy index seek ") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "CALL db.relationshipLegacyIndexSeek('relIndex', 'key', 'value') YIELD relationship AS r RETURN r"
    val result = execute(query)

    result.toList should equal(List(Map("r"-> relationship)))
  }

  test("should fail if index doesn't exist for relationship") {
    a [CypherExecutionException] should be thrownBy
      execute("""CALL db.relationshipLegacyIndexSeek('index', 'key', 'value') YIELD relationship AS r RETURN r""")
  }
}
