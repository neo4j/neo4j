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

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class ManualIndexProcsIT extends ExecutionEngineFunSuite {

  override def databaseConfig(): Map[Setting[_], String] = Map(
    GraphDatabaseSettings.node_auto_indexing -> "true",
    GraphDatabaseSettings.node_keys_indexable -> "name,email",
    GraphDatabaseSettings.relationship_auto_indexing -> "true",
    GraphDatabaseSettings.relationship_keys_indexable -> "weight")

  private def assertNodeIndexExists(indexName: String, doesit: Boolean) = {
    graph.inTx {
      graph.index().existsForNodes(indexName) should be(doesit)
    }
  }

  private def assertRelationshipIndexExists(indexName: String, doesit: Boolean) = {
    graph.inTx {
      graph.index().existsForRelationships(indexName) should be(doesit)
    }
  }

  test("Node from exact key value match") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute(
      """CALL db.index.explicit.seekNodes('index', 'key', 'value')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node seek") {
    a[CypherExecutionException] should be thrownBy
      execute(
        """CALL db.index.explicit.seekNodes('index', 'key', 'value')
          |YIELD node AS n RETURN n""".stripMargin)
  }

  test("should return node from manual index search") {
    val node = createNode()
    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
    }

    val result = execute( """CALL db.index.explicit.searchNodes("index", "key:value") YIELD node as n RETURN n""").toList

    result should equal(List(Map("n" -> node)))
  }

  test("should fail if index doesn't exist for node search") {
    a[CypherExecutionException] should be thrownBy
      execute("""CALL db.index.explicit.searchNodes('index', 'key:value') YIELD node AS n RETURN n""")
  }

  test("explicit index + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val thirdNode = createNode(Map("prop" -> 37))
    val fourthNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index().forNodes("index").add(node, "key", "value")
      graph.index().forNodes("index").add(otherNode, "key", "value")
      graph.index().forNodes("index").add(thirdNode, "key", "value")
      graph.index().forNodes("index").add(fourthNode, "key", "value")
    }

    val result = execute(
      """CALL db.index.explicit.searchNodes("index", "key:value") YIELD node AS n WHERE n.prop = 21 RETURN n""").toList

    result should equal(List(Map("n" -> otherNode), Map("n" -> fourthNode)))
  }

  test("should seek relationship from manual index") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)
    val unwantedRelationship = relate(node, otherNode)
    val ignoredRelationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
      relationshipIndex.add(unwantedRelationship, "key", "wrongValue")
      relationshipIndex.add(ignoredRelationship, "wrongKey", "value")
    }

    val query = "CALL db.index.explicit.seekRelationships('relIndex', 'key', 'value') YIELD relationship AS r RETURN r"
    val result = execute(query)

    result.toList should equal(List(Map("r" -> relationship)))
  }

  test("should fail if index doesn't exist for relationship") {
    a[CypherExecutionException] should be thrownBy
      execute("""CALL db.index.explicit.seekRelationships('index', 'key', 'value') YIELD relationship AS r RETURN r""")
  }

  test("should do manual relationship index search") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val r1 = relate(node, otherNode)
    val r2 = relate(node, otherNode)
    val r3 = relate(node, otherNode)
    val r4 = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(r1, "wrongKey", "value")
      relationshipIndex.add(r2, "key", "value")
      relationshipIndex.add(r3, "key", "wrongValue")
      relationshipIndex.add(r4, "key", "value")
    }

    val query = "CALL db.index.explicit.searchRelationships('relIndex','key:value') YIELD relationship AS r RETURN r"
    val result = execute(query)

    result.toList should equal(List(
      Map("r" -> r2),
      Map("r" -> r4)
    ))
  }

  test("should MATCH undirected using result from manual index search") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "CALL db.index.explicit.searchRelationships('relIndex','key:*') YIELD relationship AS r MATCH (a)-[r]-(b) RETURN r"
    val result = execute(query)

    result.toList should equal(List(
      Map("r" -> relationship),
      Map("r" -> relationship)
    ))
  }

  test("should MATCH directed using result from manual index search") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))
    val relationship = relate(node, otherNode)

    graph.inTx {
      val relationshipIndex = graph.index().forRelationships("relIndex")
      relationshipIndex.add(relationship, "key", "value")
    }

    val query = "CALL db.index.explicit.searchRelationships('relIndex','key:*') YIELD relationship AS r MATCH (a)-[r]->(b) RETURN r"
    val result = execute(query)

    result.toList should equal(List(
      Map("r" -> relationship)
    ))
  }

  test("should return correct results on combined node and relationship index seek") {
    val node = createNode()
    val resultNode = createNode()
    val rel = relate(node, resultNode)
    relate(node, createNode())

    graph.inTx {
      graph.index().forNodes("nodes").add(node, "key", "A")
      graph.index().forRelationships("rels").add(rel, "key", "B")
    }

    val result = execute("CALL db.index.explicit.seekNodes('nodes', 'key', 'A') YIELD node AS n " +
      "CALL db.index.explicit.seekRelationships('rels', 'key', 'B') YIELD relationship AS r " +
      "MATCH (n)-[r]->(b) RETURN b")
    result.toList should equal(List(Map("b" -> resultNode)))
  }

  test("relationship search with bound start node") {
    val node = createNode("name" -> "Neo")
    val otherNode = createNode("name" -> "Trinity")
    val rel = relate(node, otherNode)
    val otherRel = relate(node, otherNode)
    val backwardsRel = relate(otherNode, node)

    graph.inTx {
      graph.index().forRelationships("relIndex").add(rel,"weight", 41)
      graph.index().forRelationships("relIndex").add(otherRel,"length", 42)
      graph.index().forRelationships("relIndex").add(backwardsRel,"weight", 43)
    }

    val result = execute(
      "MATCH (n {name:'Neo'})" +
      "CALL db.index.explicit.searchRelationshipsIn('relIndex', n, 'weight:*') YIELD relationship as r RETURN r").toList

    result should equal(List(Map("r" -> rel)))
  }

  test("relationship search with bound end node") {
    val node = createNode("name" -> "Neo")
    val otherNode = createNode("name" -> "Trinity")
    val rel = relate(node, otherNode)
    val otherRel = relate(node, otherNode)
    val backwardsRel = relate(otherNode, node)

    graph.inTx {
      graph.index().forRelationships("relIndex").add(rel,"weight", 41)
      graph.index().forRelationships("relIndex").add(otherRel,"length", 42)
      graph.index().forRelationships("relIndex").add(backwardsRel,"weight", 43)
    }

    val result = execute(
      "MATCH (n {name:'Trinity'})" +
      "CALL db.index.explicit.searchRelationshipsOut('relIndex', n, 'weight:*') YIELD relationship as r RETURN r").toList

    result should equal(List(Map("r" -> rel)))
  }

  test("relationship search with bound start and end nodes") {
    val node = createNode("name" -> "Neo")
    val otherNode = createNode("name" -> "Trinity")
    val thirdNode = createNode("name" -> "Spoon")
    val rel1 = relate(node, otherNode)
    val rel2 = relate(node, otherNode)
    val rel3 = relate(otherNode, node)
    val rel4 = relate(node, thirdNode)
    val rel5 = relate(node, thirdNode)
    val rel6 = relate(thirdNode, node)

    graph.inTx {
      graph.index().forRelationships("relIndex").add(rel1,"weight", 41)
      graph.index().forRelationships("relIndex").add(rel2,"length", 42)
      graph.index().forRelationships("relIndex").add(rel3,"weight", 43)
      graph.index().forRelationships("relIndex").add(rel4,"weight", 41)
      graph.index().forRelationships("relIndex").add(rel5,"length", 42)
      graph.index().forRelationships("relIndex").add(rel6,"weight", 43)
    }

    val result = execute(
      "MATCH (n1 {name:'Neo'})" +
      "MATCH (n2 {name:'Spoon'})" +
      "CALL db.index.explicit.searchRelationshipsBetween('relIndex', n1, n2, 'weight:*') YIELD relationship as r RETURN r").toList

    result should equal(List(Map("r" -> rel4)))
  }

  test("Auto-index node from exact key value match using manual feature") {
    val node = createNode(Map("name" -> "Neo"))

    val result = execute(
      """CALL db.index.explicit.seekNodes('node_auto_index', 'name', 'Neo')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("Auto-index relationship from exact key value match using manual seek") {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "weight" -> 12)

    val result = execute(
      """CALL db.index.explicit.seekRelationships('relationship_auto_index', 'weight', 12)
        |YIELD relationship AS r RETURN r""".stripMargin).toList

    result should equal(List(Map("r" -> rel)))
  }

  test("Auto-index and return node using auto seek") {
    createNode(Map("wrongName" -> "Neo"))
    val node = createNode(Map("name" -> "Neo"))
    createNode(Map("name" -> "wrongKey"))

    val result = execute(
      """CALL db.index.explicit.auto.seekNodes('name', 'Neo')
        |YIELD node AS n RETURN n""".stripMargin).toList

    result should equal(List(Map("n" -> node)))
  }

  test("Auto-index and return node using auto search") {
    val node = createNode(Map("name" -> "Neo"))
    createNode(Map("name" -> "Johan"))
    val otherNode = createNode(Map("name" -> "Nina"))

    val result = execute("CALL db.index.explicit.auto.searchNodes('name:N*') YIELD node as n RETURN n").toList

    result should equal(List(Map("n" -> node), Map("n" -> otherNode)))
  }

  test("Auto-index and return relationship using auto search") {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "weight" -> 42)
    val rel2 = relate(a, b, "weight" -> 37)
    val rel3 = relate(a, b, "weight" -> 3)
    val rel4 = relate(a, b, "width" -> 3)

    val result = execute(
      """CALL db.index.explicit.auto.searchRelationships('weight:3*')
    |YIELD relationship AS r RETURN r""".stripMargin).toList

    result should equal(List(Map("r" -> rel2), Map("r" -> rel3)))
  }

  test("Auto-index relationship from exact key value match using auto feature") {
    val a = createNode()
    val b = createNode()
    val rel = relate(a, b, "weight" -> 12)
    relate(a, b, "height" -> 12)
    relate(a, b, "weight" -> 10)

    val result = execute(
      """CALL db.index.explicit.auto.seekRelationships('weight', 12)
        |YIELD relationship AS r RETURN r""".stripMargin).toList

    result should equal(List(Map("r" -> rel)))
  }

  test("Should create a node manual index using a procedure") {
    // Given a database with nodes with properties
    val node = createNode(Map("name" -> "Neo"))

    // When adding a node to the index, the index should exist
    val addResult = execute(
      """MATCH (n) WITH n CALL db.index.explicit.addNode('usernames', n, 'name', 'Neo') YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    // Then the index should exist
    val result = execute("CALL db.index.explicit.existsForNodes('usernames')").toList

    result should be(List(Map("success" -> true)))

    // And queries should return nodes
    graph.inTx {
      val results = graph.index().forNodes("usernames").get("name", "Neo")
      results.size() should be(1)
      results.getSingle should be(node)
    }
  }

  test("Should create a relationship manual index using a procedure") {
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    val addResult = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.index.explicit.addRelationship('relIndex', r, 'distance', 12) YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val result = execute(
      """CALL db.index.explicit.existsForRelationships('relIndex')
        |YIELD success AS s RETURN s""".stripMargin).toList

    result should equal(List(Map("s" -> true)))
  }

  test("Should be able to drop node index") {
    // Given a database with nodes with properties
    val node = createNode(Map("name" -> "Neo"))

    // When adding a node to the index
    graph.inTx {
      graph.index().forNodes("usernames").add(node, "name", "Neo")
    }

    // Then the index should be possible to drop
    val result = execute("CALL db.index.explicit.drop('usernames')").toList

    result should be(List(Map("name" -> "usernames", "type" -> "NODE", "config" -> Map("provider" -> "lucene", "type" -> "exact"))))
  }

  test("Should be able to drop relationship index") {
    // Given a relationship with property
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    // When adding the relationship to an index
    graph.inTx {
      graph.index().forRelationships("relIndex").add(rel, "distance", 12)
    }

    // Then the index should be possible to drop
    val result = execute("CALL db.index.explicit.drop('relIndex')").toList

    result should be(List(Map("name" -> "relIndex", "type" -> "RELATIONSHIP", "config" -> Map("provider" -> "lucene", "type" -> "exact"))))
  }

  test("Should able to add and remove a node from manual index") {
    val node = createNode(Map("name" -> "Neo"))

    val addResult = execute(
      """MATCH (n) WITH n CALL db.index.explicit.addNode('usernames', n, 'name', 'Neo') YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.index.explicit.seekNodes('usernames', 'name', 'Neo') YIELD node AS n ").toList

    seekResult should equal(List(Map("n" -> node)))

    val result = execute(
      """MATCH (n) WITH n CALL db.index.explicit.removeNode('usernames', n, 'name') YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.index.explicit.seekNodes('usernames', 'name', 'Neo') YIELD node AS n ").toList

    emptyResult should equal(List.empty)
  }

  test("Should able to add and remove a node from manual index using default parameter") {
    val node = createNode(Map("name" -> "Neo"))

    val addResult = execute(
      """MATCH (n) WITH n CALL db.index.explicit.addNode('usernames', n, 'name', 'Neo') YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.index.explicit.seekNodes('usernames', 'name', 'Neo') YIELD node AS n ").toList

    seekResult should equal(List(Map("n" -> node)))

    val result = execute(
      """MATCH (n) WITH n CALL db.index.explicit.removeNode('usernames', n) YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.index.explicit.seekNodes('usernames', 'name', 'Neo') YIELD node AS n ").toList

    emptyResult should equal(List.empty)
  }

  test("Should able to add and remove a relationship from manual index") {
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    val addResult = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.index.explicit.addRelationship('relIndex', r, 'distance', 12) YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.index.explicit.seekRelationships('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    seekResult should equal(List(Map("r" -> rel)))

    val result = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.index.explicit.removeRelationship('relIndex', r, 'distance') YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.index.explicit.seekRelationships('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    emptyResult should equal(List.empty)
  }

  test("Should able to add and remove a relationship from manual index using default parameter") {
    val a = createNode(Map("name" -> "Neo"))
    val b = createNode()
    val rel = relate(a, b, "distance" -> 12)

    val addResult = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.index.explicit.addRelationship('relIndex', r, 'distance', 12) YIELD success as s RETURN s"""
        .stripMargin).toList

    addResult should be(List(Map("s" -> true)))

    val seekResult = execute("CALL db.index.explicit.seekRelationships('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    seekResult should equal(List(Map("r" -> rel)))

    val result = execute(
      """MATCH (n)-[r]-(m) WHERE n.name = 'Neo' WITH r CALL db.index.explicit.removeRelationship('relIndex', r) YIELD success as s RETURN s"""
        .stripMargin).toList

    result should equal(List(Map("s" -> true)))

    val emptyResult = execute("CALL db.index.explicit.seekRelationships('relIndex', 'distance', '12') YIELD relationship AS r ").toList

    emptyResult should equal(List.empty)
  }

  test("should be able to get or create a node index") {
    //Given
    assertNodeIndexExists("usernames", false)

    //When
    val result = execute("CALL db.index.explicit.forNodes('usernames') YIELD type, name").toList
    result should equal(List(Map("name" -> "usernames", "type" -> "NODE")))

    //Then
    assertNodeIndexExists("usernames", true)
  }

  test("should be able to get an existing node index without specifying a configuration") {
    //Given
    assertNodeIndexExists("usernames", false)
    execute("CALL db.index.explicit.forNodes('usernames') YIELD type, name").toList

    //When
    val result = execute("CALL db.index.explicit.forNodes('usernames') YIELD type, name").toList
    result should equal(List(Map("name" -> "usernames", "type" -> "NODE")))

    //Then
    assertNodeIndexExists("usernames", true)
  }

  test("should be able to get an existing node index with specifying a configuration") {
    //Given
    assertNodeIndexExists("usernames", false)
    execute("CALL db.index.explicit.forNodes('usernames') YIELD type, name").toList

    //When
    val result = execute("CALL db.index.explicit.forNodes('usernames', {type: 'exact', provider: 'lucene'}) YIELD type, name").toList
    result should equal(List(Map("name" -> "usernames", "type" -> "NODE")))

    //Then
    assertNodeIndexExists("usernames", true)
  }

  test("cannot get a node index with a different type if it exists already") {
    //Given
    assertNodeIndexExists("usernames", false)

    //Then
    val result1 = execute("CALL db.index.explicit.forNodes('usernames', {type: 'exact', provider: 'lucene'}) YIELD type, name, config").toList
    result1.head("config").asInstanceOf[Map[String, String]] should contain("type" -> "exact")
    result1.head("config").asInstanceOf[Map[String, String]] should contain("provider" -> "lucene")
    intercept[Exception] {
      execute("CALL db.index.explicit.forNodes('usernames', {type: 'fulltext', provider: 'lucene'}) YIELD type, name, config").toList
    }.getMessage should include("doesn't match stored config in a valid way")

    //And Then
    assertNodeIndexExists("usernames", true)
  }

  test("configuring a fulltext index should enable fulltext querying") {
    //Given
    assertNodeIndexExists("usernames", false)

    execute("CALL db.index.explicit.forNodes('usernames', {type: 'fulltext', provider: 'lucene'}) YIELD type, name, config").toList
    assertNodeIndexExists("usernames", true)

    execute("CREATE (n {prop:'x'}) WITH n CALL db.index.explicit.addNode('usernames',n,'username','A Satia be') YIELD success RETURN success").toList
    // will only find the node with fulltext index, not with exact
    execute("CALL db.index.explicit.searchNodes('usernames', 'username:Satia') YIELD node RETURN node.prop").columnAs("node.prop").toList should be(List("x"))
  }

  test("wrong index type does not lead to null-pointer-exception") {
    val e = intercept[Exception](
      execute("CALL db.index.explicit.forNodes('usernames', {type: 'does not exist', provider: 'lucene'}) YIELD type, name, config").toList
    )
    e.getCause.getCause.getMessage should equal("The given type was not recognized: does not exist. Known types are 'fulltext' and 'exact'")
  }

  test("should be able to get or create a relationship index") {
    //Given
    assertRelationshipIndexExists("relIndex", false)

    //When
    val result = execute("CALL db.index.explicit.forRelationships('relIndex') YIELD type, name").toList
    result should equal(List(Map("name" -> "relIndex", "type" -> "RELATIONSHIP")))

    //Then
    assertRelationshipIndexExists("relIndex", true)
  }

  test("should be able to get an existing relationship index without specifying a configuration") {
    //Given
    assertRelationshipIndexExists("relIndex", false)
    execute("CALL db.index.explicit.forRelationships('relIndex') YIELD type, name").toList

    //When
    val result = execute("CALL db.index.explicit.forRelationships('relIndex') YIELD type, name").toList
    result should equal(List(Map("name" -> "relIndex", "type" -> "RELATIONSHIP")))

    //Then
    assertRelationshipIndexExists("relIndex", true)
  }

  test("should be able to get an existing relationship index with specifying a configuration") {
    //Given
    assertRelationshipIndexExists("relIndex", false)
    execute("CALL db.index.explicit.forRelationships('relIndex') YIELD type, name").toList

    //When
    val result = execute("CALL db.index.explicit.forRelationships('relIndex', {type: 'exact', provider: 'lucene'}) YIELD type, name").toList
    result should equal(List(Map("name" -> "relIndex", "type" -> "RELATIONSHIP")))

    //Then
    assertRelationshipIndexExists("relIndex", true)
  }

  test("cannot get a relationship index with a different type if it exists already") {
    //Given
    assertRelationshipIndexExists("relIndex", false)

    //Then
    val result1 = execute("CALL db.index.explicit.forRelationships('relIndex', {type: 'exact', provider: 'lucene'}) YIELD type, name, config").toList
    result1.head("config").asInstanceOf[Map[String, String]] should contain("type" -> "exact")
    result1.head("config").asInstanceOf[Map[String, String]] should contain("provider" -> "lucene")
    intercept[Exception] {
      execute("CALL db.index.explicit.forRelationships('relIndex', {type: 'fulltext', provider: 'lucene'}) YIELD type, name, config").toList
    }.getMessage should include("doesn't match stored config in a valid way")

    //And Then
    assertRelationshipIndexExists("relIndex", true)
  }

  test("manual node index exists") {
    val result = execute("CALL db.index.explicit.existsForNodes('nodeIndex') YIELD success").toList
    result should equal(List(Map("success" -> false)))

    graph.inTx {
      graph.index().forNodes("nodeIndex")
    }

    val result2 = execute("CALL db.index.explicit.existsForNodes('nodeIndex') YIELD success").toList
    result2 should equal(List(Map("success" -> true)))
  }

  test("manual relationship index exists") {
    val result = execute("CALL db.index.explicit.existsForRelationships('relationshipIndex') YIELD success").toList
    result should equal(List(Map("success" -> false)))

    graph.inTx {
      graph.index().forRelationships("relationshipIndex")
    }

    val result2 = execute("CALL db.index.explicit.existsForRelationships('relationshipIndex') YIELD success").toList
    result2 should equal(List(Map("success" -> true)))
  }

  test("should be able to list manual and automatic indexes") {
    //Given the node and relationship indexes do not exist
    graph.inTx {
      graph.index().nodeIndexNames().length should be(0)
    }

    //When creating indexes both manually and automatically
    graph.execute("CALL db.index.explicit.forNodes('manual1')")
    graph.execute("CALL db.index.explicit.forRelationships('manual2')")
    graph.execute("CREATE (n) WITH n CALL db.index.explicit.addNode('usernames',n,'username','Neo') YIELD success RETURN success")
    graph.execute("CREATE (n), (m), (n)-[r:KNOWS]->(m) WITH r CALL db.index.explicit.addRelationship('relIndex',r,'distance',42) YIELD success RETURN success")
    graph.execute("CREATE (n {email:'joe@soap.net'})")
    graph.execute("CREATE (n), (m), (n)-[r:KNOWS {weight:42}]->(m)")

    //Then the indexes should all exist
    graph.inTx {
      graph.index().nodeIndexNames().toSet should be(Set("manual1",  "usernames", "node_auto_index"))
      graph.index().relationshipIndexNames().toSet should be(Set("manual2", "relIndex", "relationship_auto_index"))
    }

    //And have the right types
    val result = execute("CALL db.index.explicit.list").toSet
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


