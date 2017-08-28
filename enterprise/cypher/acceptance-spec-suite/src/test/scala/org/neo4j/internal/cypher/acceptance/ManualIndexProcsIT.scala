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
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class ManualIndexProcsIT extends ExecutionEngineFunSuite {

  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.node_auto_indexing -> "true",
    GraphDatabaseSettings.node_keys_indexable -> "name,email",
    GraphDatabaseSettings.relationship_auto_indexing -> "true",
    GraphDatabaseSettings.relationship_keys_indexable -> "weight")

  test("Node from exact key value match") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute(
      """CALL db.nodeManualIndexSeek('index', 'key', 'value')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node seek") {
    a[CypherExecutionException] should be thrownBy
      execute(
        """CALL db.nodeManualIndexSeek('index', 'key', 'value')
          |YIELD node AS n RETURN n""".stripMargin)
  }

  test("Node from query lucene index") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute( """CALL db.nodeManualIndexSearch("index", "key:value") YIELD node as n RETURN n""").toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node search") {
    a[CypherExecutionException] should be thrownBy
      execute("""CALL db.nodeManualIndexSearch('index', 'key:value') YIELD node AS n RETURN n""")
  }

  test("legacy index + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
      graph.index().forNodes("index").add(otherNode, "key", "value")
    }

    val result = execute(
      """CALL db.nodeManualIndexSearch("index", "key:value") YIELD node AS n WHERE n.prop = 42 RETURN n""").toList

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

    val query = "CALL db.relationshipManualIndexSeek('relIndex', 'key', 'value') YIELD relationship AS r RETURN r"
    val result = execute(query)

    result.toList should equal(List(Map("r" -> relationship)))
  }

  test("should fail if index doesn't exist for relationship") {
    a[CypherExecutionException] should be thrownBy
      execute("""CALL db.relationshipManualIndexSeek('index', 'key', 'value') YIELD relationship AS r RETURN r""")
  }

  test("Relationship legacy index search plus MATCH") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "CALL db.relationshipManualIndexSearch('relIndex','key:*') YIELD relationship AS r MATCH (a)-[r]-(b) RETURN r"
    val result = execute(query)

    result.toList should equal(List(
      Map("r" -> relationship),
      Map("r" -> relationship)
    ))
  }

  test("Relationship legacy index search plus MATCH directed") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "CALL db.relationshipManualIndexSearch('relIndex','key:*') YIELD relationship AS r MATCH (a)-[r]->(b) RETURN r"
    val result = execute(query)

    result.toList should equal(List(
      Map("r" -> relationship)
    ))
  }

  test("should return correct results on combined node and relationship index starts") {
    val node = createNode()
    val resultNode = createNode()
    val rel = relate(node, resultNode)
    relate(node, createNode())

    graph.inTx {
      graph.index().forNodes("nodes").add(node, "key", "A")
      graph.index().forRelationships("rels").add(rel, "key", "B")
    }

    val result = execute("CALL db.nodeManualIndexSeek('nodes', 'key', 'A') YIELD node AS n " +
      "CALL db.relationshipManualIndexSeek('rels', 'key', 'B') YIELD relationship AS r " +
      "MATCH (n)-[r]->(b) RETURN b")
    result.toList should equal(List(Map("b" -> resultNode)))
  }

  test("Auto-index node from exact key value match using manual feature") {
    val node = createNode(Map("name" -> "Neo"))

    val result = execute(
      """CALL db.nodeManualIndexSeek('node_auto_index', 'name', 'Neo')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("Auto-index relationship from exact key value match using manual feature") {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "weight" -> 12)

    val result = execute(
      """CALL db.relationshipManualIndexSeek('relationship_auto_index', 'weight', 12)
        |YIELD relationship AS r RETURN r""".stripMargin).toList

    result should equal(List(Map("r" -> rel)))
  }

  test("Auto-index node from exact key value match using auto feature") {
    val node = createNode(Map("name" -> "Neo"))

    val result = execute(
      """CALL db.nodeAutoIndexSeek('name', 'Neo')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("Auto-index relationship from exact key value match using auto feature") {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "weight" -> 12)

    val result = execute(
      """CALL db.relationshipAutoIndexSeek('weight', 12)
        |YIELD relationship AS r RETURN r""".stripMargin).toList

    result should equal(List(Map("r" -> rel)))
  }

  test("Should be able to create a node manual index by using a procedure") {
    // Given a database with nodes with properties
    val node = createNode(Map("name" -> "Neo"))

    // When adding a node to the index, the index should exist
    val addResult = execute(
      """MATCH (n) WITH n CALL db.nodeManualIndexAdd('usernames', n, 'name', 'Neo') YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    // Then the index should exist
    val result = execute("CALL db.nodeManualIndexExists('usernames')").toList

    result should be(List(Map("success" -> true)))

    // And queries should return nodes
    graph.inTx {
      val results = graph.index().forNodes("usernames").get("name", "Neo")
      results.size() should be(1)
      results.getSingle should be(node)
    }
  }

  test("Maunal relationships index should exist") {
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    val addResult = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.relationshipManualIndexAdd('relIndex', r, 'distance', 12) YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val result = execute(
      """CALL db.relationshipManualIndexExists('relIndex')
        |YIELD success AS s RETURN s""".stripMargin).toList

    result should equal(List(Map("s" -> true)))
  }

  test("Should be able to drop node index") {
    // Given a database with nodes with properties
    val node = createNode(Map("name" -> "Neo"))

    // When adding a node to the index
    graph.inTx {
      graph.index().forNodes("usernames").add(node, "name", "Neo");
    }

    // Then the index should be possible to drop
    val result = execute("CALL db.manualIndexDrop('usernames')").toList

    result should be(List(Map("name" -> "usernames", "type" -> "NODE", "config" -> Map("provider" -> "lucene", "type" -> "exact"))))
  }

  test("Should be able to drop relationship index") {
    // Given a relationship with property
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    // When adding the relationship to an index
    graph.inTx {
      graph.index().forRelationships("relIndex").add(rel, "distance", 12);
    }

    // Then the index should be possible to drop
    val result = execute("CALL db.manualIndexDrop('relIndex')").toList

    result should be(List(Map("name" -> "relIndex", "type" -> "RELATIONSHIP", "config" -> Map("provider" -> "lucene", "type" -> "exact"))))
  }

  test("Should able to add and remove a node from manual index") {
    val node = createNode(Map("name" -> "Neo"))

    val addResult = execute(
      """MATCH (n) WITH n CALL db.nodeManualIndexAdd('usernames', n, 'name', 'Neo') YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.nodeManualIndexSeek('usernames', 'name', 'Neo') YIELD node AS n ").toList

    seekResult should equal(List(Map("n" -> node)))

    val result = execute(
      """MATCH (n) WITH n CALL db.nodeManualIndexRemove('usernames', n, 'name') YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.nodeManualIndexSeek('usernames', 'name', 'Neo') YIELD node AS n ").toList

    emptyResult should equal(List.empty)

  }

  test("Should able to add and remove a relationship from manual index") {
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    val addResult = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.relationshipManualIndexAdd('relIndex', r, 'distance', 12) YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.relationshipManualIndexSeek('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    seekResult should equal(List(Map("r" -> rel)))

    val result = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.relationshipManualIndexRemove('relIndex', r, 'distance') YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.relationshipManualIndexSeek('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    emptyResult should equal(List.empty)

  }

  test("should be able to get or create a node index") {
    //Given the node index does not exist
    graph.inTx {
      graph.index().existsForNodes("usernames") should be(false)
    }

    //When calling nodeManualIndex
    graph.execute("CALL db.nodeManualIndex('usernames')")

    //Then the index should exist
    graph.inTx {
      graph.index().existsForNodes("usernames") should be(true)
    }
  }

  test("should be able to get or create a relationship index") {
    //Given the relationship index does not exist
    graph.inTx {
      graph.index().existsForRelationships("relIndex") should be(false)
    }

    //When calling nodeManualIndex
    graph.execute("CALL db.relationshipManualIndex('relIndex')")

    //Then the index should exist
    graph.inTx {
      graph.index().existsForRelationships("relIndex") should be(true)
    }
  }

  test("should be able to list manual and automatic indexes") {
    //Given the node and relationship indexes do not exist
    graph.inTx {
      graph.index.nodeIndexNames().length should be(0)
    }

    //When creating indexes both manually and automatically
    graph.execute("CALL db.nodeManualIndex('manual1')")
    graph.execute("CALL db.relationshipManualIndex('manual2')")
    graph.execute("CREATE (n) WITH n CALL db.nodeManualIndexAdd('usernames',n,'username','Neo') YIELD success RETURN success")
    graph.execute("CREATE (n), (m), (n)-[r:KNOWS]->(m) WITH r CALL db.relationshipManualIndexAdd('relIndex',r,'distance',42) YIELD success RETURN success")
    graph.execute("CREATE (n {email:'joe@soap.net'})")
    graph.execute("CREATE (n), (m), (n)-[r:KNOWS {weight:42}]->(m)")

    //Then the indexes should all exist
    graph.inTx {
      graph.index.nodeIndexNames().toSet should be(Set("manual1",  "usernames", "node_auto_index"))
      graph.index.relationshipIndexNames().toSet should be(Set("manual2", "relIndex", "relationship_auto_index"))
    }

    //And have the right types
    val result = execute("CALL db.manualIndexes").toSet
    result should be(Set(
      Map("name" -> "manual1", "type" -> "NODE", "config" -> Map("provider" -> "lucene", "type" -> "exact")),
      Map("name" -> "manual2", "type" -> "RELATIONSHIP", "config" -> Map("provider" -> "lucene", "type" -> "exact")),
      Map("name" -> "usernames", "type" -> "NODE", "config" -> Map("provider" -> "lucene", "type" -> "exact")),
      Map("name" -> "relIndex", "type" -> "RELATIONSHIP", "config" -> Map("provider" -> "lucene", "type" -> "exact")),
      Map("name" -> "node_auto_index", "type" -> "NODE", "config" -> Map("provider" -> "lucene", "type" -> "exact")),
      Map("name" -> "relationship_auto_index", "type" -> "RELATIONSHIP", "config" -> Map("provider" -> "lucene", "type" -> "exact"))
    ))
  }
}


