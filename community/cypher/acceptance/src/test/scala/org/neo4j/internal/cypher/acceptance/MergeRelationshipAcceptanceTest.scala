/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport, SyntaxException}
import org.neo4j.graphdb.{Path, Relationship}

class MergeRelationshipAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("should_be_able_to_create_relationship") {
    // given
    val a = createNode("A")
    val b = createNode("B")

    // when
    val r = executeScalar("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) RETURN r").asInstanceOf[Relationship]

    // then
    graph.inTx {
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
      r.getType.name() should equal("TYPE")
    }
  }

  test("should_be_able_to_find_a_relationship") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    val r1 = relate(a, b, "TYPE")

    // when
    val result = executeScalar("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) RETURN r").asInstanceOf[Relationship]

    // then
    result should equal(r1)
  }

  test("should_be_able_to_find_two_existing_relationships") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    val r1 = relate(a, b, "TYPE")
    val r2 = relate(a, b, "TYPE")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) RETURN r").columnAs[Relationship]("r").toList

    // then
    result should equal(List(r2, r1))
  }

  test("should_be_able_to_find_two_relationships") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    val r1 = relate(a, b, "TYPE")
    val r2 = relate(a, b, "TYPE")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) RETURN r").columnAs[Relationship]("r")

    // then
    result.toSet should equal(Set(r1, r2))
  }

  test("should_be_able_to_filter_out_relationships") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE", "r1")
    val r = relate(a, b, "TYPE", "r2")

    // when
    val result = executeScalar("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE {name:'r2'}]->(b) RETURN r").asInstanceOf[Relationship]

    // then
    result should equal(r)
  }

  test("should_be_able_to_create_when_nothing_matches") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE", "r1")

    // when
    val r = executeScalar("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE {name:'r2'}]->(b) RETURN r").asInstanceOf[Relationship]

    // then
    graph.inTx {
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
      r.getType.name() should equal("TYPE")
    }
  }

  test("should_not_be_fooled_by_direction") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    val r = relate(b, a, "TYPE")
    val r2 = relate(a, b, "TYPE")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)<-[r:TYPE]-(b) RETURN r")

    // then
    assertStats(result, relationshipsCreated = 0)
    result.toList should equal(List(Map("r" -> r)))
  }

  test("should_create_relationship_with_property") {
    // given
    val a = createNode("A")
    val b = createNode("B")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE {name:'Lola'}]->(b) RETURN r")

    // then
    assertStats(result, relationshipsCreated = 1, propertiesSet = 1)
    graph.inTx {
      val r = result.toList.head("r").asInstanceOf[Relationship]
      r.getProperty("name") should equal("Lola")
      r.getType.name() should equal("TYPE")
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
    }
  }

  test("should_handle_on_create") {
    // given
    val a = createNode("A")
    val b = createNode("B")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'Lola' RETURN r")

    // then
    assertStats(result, relationshipsCreated = 1, propertiesSet = 1)
    graph.inTx {
      val r = result.toList.head("r").asInstanceOf[Relationship]
      r.getProperty("name") should equal("Lola")
      r.getType.name() should equal("TYPE")
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
    }
  }

  test("should_handle_on_match") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE")

    // when
    val result = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON MATCH SET r.name = 'Lola' RETURN r")

    // then
    assertStats(result, relationshipsCreated = 0, propertiesSet = 1)
    graph.inTx {
      val r = result.toList.head("r").asInstanceOf[Relationship]
      r.getProperty("name") should equal("Lola")
      r.getType.name() should equal("TYPE")
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
    }
  }

  test("should_work_with_single_bound_node") {
    // given
    val a = createNode("A")

    // when
    val result = execute("MATCH (a {name:'A'}) MERGE (a)-[r:TYPE]->() RETURN r")

    // then
    assertStats(result, relationshipsCreated = 1, nodesCreated = 1)
    graph.inTx {
      val r = result.toList.head("r").asInstanceOf[Relationship]
      r.getType.name() should equal("TYPE")
      r.getStartNode should equal(a)
    }
  }

  test("should_handle_longer_patterns") {
    // given
    val a = createNode("A")

    // when
    val result = execute("MATCH (a {name:'A'}) MERGE (a)-[r:TYPE]->()<-[:TYPE]-(b) RETURN r")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2)
    graph.inTx {
      val r = result.toList.head("r").asInstanceOf[Relationship]
      r.getType.name() should equal("TYPE")
      r.getStartNode should equal(a)
    }
  }

  test("should_handle_nodes_bound_in_the_middle") {
    // given
    val b = createNode("B")

    // when
    val result = execute("MATCH (b {name:'B'}) MERGE (a)-[r1:TYPE]->(b)<-[r2:TYPE]-(c) RETURN r1, r2")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2)
    val resultMap = result.toList.head
    graph.inTx {
      val r1 = resultMap("r1").asInstanceOf[Relationship]
      r1.getType.name() should equal("TYPE")
      r1.getEndNode should equal(b)

      val r2 = resultMap("r2").asInstanceOf[Relationship]
      r2.getType.name() should equal("TYPE")
      r2.getEndNode should equal(b)
    }
  }

  test("should_handle_nodes_bound_in_the_middle_when_half_pattern_is_matching") {
    // given
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    relate(a, b, "TYPE")

    // when
    val result = execute("MATCH (b:B) MERGE (a:A)-[r1:TYPE]->(b)<-[r2:TYPE]-(c:C) RETURN r1, r2")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, labelsAdded = 2)
    val resultMap = result.toList.head
    graph.inTx {
      val r1 = resultMap("r1").asInstanceOf[Relationship]
      r1.getType.name() should equal("TYPE")
      r1.getEndNode should equal(b)

      val r2 = resultMap("r2").asInstanceOf[Relationship]
      r2.getType.name() should equal("TYPE")
      r2.getEndNode should equal(b)
    }
  }

  test("should_handle_first_declaring_nodes_and_then_creating_relationships_between_them") {
    // given
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")

    // when
    val result = execute("MERGE (a:A) MERGE (b:B) MERGE (a)-[:FOO]->(b)")

    // then
    assertStats(result, relationshipsCreated = 1)
  }

  test("should_handle_building_links_mixing_create_with_merge_pattern") {
    // given

    // when
    val result = execute("CREATE (a:A) MERGE (a)-[:KNOWS]->(b:B) CREATE (b)-[:KNOWS]->(c:C) RETURN a, b, c")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 3, labelsAdded = 3)
  }

  test("when_merging_a_pattern_that_includes_a_unique_node_constraint_violation_fail") {
    // given
    graph.createConstraint("Person", "id")
    createLabeledNode(Map("id"->666), "Person")

    // when then fails
    intercept[CypherExecutionException](execute("CREATE (a:A) MERGE (a)-[:KNOWS]->(b:Person {id:666})"))
  }

  test("should_work_well_inside_foreach") {
    val a = createLabeledNode("Start")
    relate(a, createNode("prop" -> 2), "FOO")

    val result = execute("match (a:Start) foreach(x in [1,2,3] | merge (a)-[:FOO]->({prop: x}) )")
    assertStats(result, nodesCreated = 2, propertiesSet = 2, relationshipsCreated = 2)
  }

  test("should_handle_two_merges_inside_foreach") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode(Map("prop" -> 42), "End")

    val result = execute("match (a:Start) foreach(x in [42] | merge (b:End {prop: x}) merge (a)-[:FOO]->(b) )")
    assertStats(result, nodesCreated = 0, propertiesSet = 0, relationshipsCreated = 1)

    graph.inTx {
      val rel = a.getRelationships.iterator().next()
      rel.getStartNode should equal(a)
      rel.getEndNode should equal(b)
    }
  }

  test("should_handle_two_merges_inside_bare_foreach") {
    createNode("x" -> 1)

    val result = execute("foreach(v in [1, 2] | merge (a {x: v}) merge (b {y: v}) merge (a)-[:FOO]->(b))")
    assertStats(result, nodesCreated = 3, propertiesSet = 3, relationshipsCreated = 2)
  }

  test("should_handle_two_merges_inside_foreach_after_with") {
    val result = execute("with 3 as y " +
      "foreach(x in [1, 2] | " +
      "merge (a {x: x, y: y}) " +
      "merge (b {x: x+1, y: y}) " +
      "merge (a)-[:FOO]->(b))")
    assertStats(result, nodesCreated = 3, propertiesSet = 6, relationshipsCreated = 2)
  }

  test("should_introduce_named_paths1") {
    val result = execute("merge (a) merge p = (a)-[:R]->() return p")
    assertStats(result, relationshipsCreated = 1, nodesCreated = 2)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  test("should_introduce_named_paths2") {
    val result = execute("merge (a { x:1 }) merge (b { x:2 }) merge p = (a)-[:R]->(b) return p")
    assertStats(result, relationshipsCreated = 1, nodesCreated = 2, propertiesSet = 2)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  test("should_introduce_named_paths3") {
    val result = execute("merge p = (a { x:1 }) return p")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  test("should_handle_foreach_in_foreach_game_of_life_ftw") {

    /* creates a grid 4 nodes wide and 4 nodes deep.
     o-o-o-o
     | | | |
     o-o-o-o
     | | | |
     o-o-o-o
     | | | |
     o-o-o-o
     */

    val result = execute(
      "foreach(x in [0,1,2] |" +
        "foreach(y in [0,1,2] |" +
        "  merge (a {x:x, y:y})" +
        "  merge (b {x:x+1, y:y})" +
        "  merge (c {x:x, y:y+1})" +
        "  merge (d {x:x+1, y:y+1})" +
        "  merge (a)-[:R]->(b)" +
        "  merge (a)-[:R]->(c)" +
        "  merge (b)-[:R]->(d)" +
        "  merge (c)-[:R]->(d)))")

    assertStats(result, nodesCreated = 16, relationshipsCreated = 24, propertiesSet = 16 * 2)
  }

  test("should_handle_merge_with_no_known_points") {
    val result = execute("merge ({name:'Andres'})-[:R]->({name:'Emil'})")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 2)
  }

  test("should_handle_foreach_in_foreach_game_without_known_points") {

    /* creates a grid 4 nodes wide and 4 nodes deep.
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     */

    val result = execute(
      "foreach(x in [0,1,2] |" +
        "foreach(y in [0,1,2] |" +
        "  merge (a {x:x, y:y})-[:R]->(b {x:x+1, y:y})" +
        "  merge (c {x:x, y:y+1})-[:R]->(d {x:x+1, y:y+1})" +
        "  merge (a)-[:R]->(c)" +
        "  merge (b)-[:R]->(d)))")

    assertStats(result, nodesCreated = 6*4, relationshipsCreated = 3*4+6*3, propertiesSet = 6*4*2)
  }

  test("should_handle_on_create_on_created_nodes") {
    val result = execute("merge (a)-[:KNOWS]->(b) ON CREATE SET b.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)
  }

  test("should_handle_on_match_on_created_nodes") {
    val result = execute("merge (a)-[:KNOWS]->(b) ON MATCH SET b.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 0)
  }

  test("should_handle_on_create_on_created_rels") {
    val result = execute("merge (a)-[r:KNOWS]->(b) ON CREATE SET r.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)
  }

  test("should_handle_on_match_on_created_rels") {
    val result = execute("merge (a)-[r:KNOWS]->(b) ON MATCH SET r.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 0)
  }

  test("should_use_left_to_right_direction_when_creating_based_on_pattern_with_undirected_relationship") {
    val result = executeScalar[Relationship]("merge (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    graph.inTx {
      result.getEndNode.getProperty("id") should equal(1)
      result.getStartNode.getProperty("id") should equal(2)
    }
  }

  test("should_find_existing_right_to_left_relationship_when_matching_with_undirected_relationship") {
    val r = relate(createNode("id" -> 1), createNode("id" -> 2), "KNOWS")
    val result = executeScalar[Relationship]("merge (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    result should equal(r)
  }

  test("should_find_existing_left_to_right_relationship_when_matching_with_undirected_relationship") {
    val r = relate(createNode("id" -> 2), createNode("id" -> 1), "KNOWS")
    val result = executeScalar[Relationship]("merge (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    result should equal(r)
  }

  test("should_find_existing_relationships_when_matching_with_undirected_relationship") {
    val r1 = relate(createNode("id" -> 2), createNode("id" -> 1), "KNOWS")
    val r2 = relate(createNode("id" -> 1), createNode("id" -> 2), "KNOWS")
    val result = execute("merge (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r").columnAs[Relationship]("r").toSet

    result should equal(Set(r1, r2))
  }

  test("should_reject_merging_nodes_having_the_same_id_but_different_labels") {
    intercept[SyntaxException]{
      execute("merge (a: Foo)-[r:KNOWS]->(a: Bar)")
    }
  }

  test("merge should handle array properties properly from identifier") {
    val query =
      """
        |CREATE (a:Foo),(b:Bar) WITH a,b
        |UNWIND ["a,b","a,b"] AS str WITH a,b,split(str,",") AS roles
        |MERGE (a)-[r:FB {foobar:roles}]->(b)
        |RETURN a,b,r""".stripMargin

    val result = execute(query)
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1, labelsAdded = 2)
  }

  test("merge should handle array properties properly") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T", Map("prop" -> Array(42, 43)))
    val result = execute("MATCH (a:A),(b:B) MERGE (a)-[r:T {prop: [42,43]}]->(b) RETURN count(*)")
    assertStats(result, nodesCreated = 0, relationshipsCreated = 0, propertiesSet = 0)
  }
}
