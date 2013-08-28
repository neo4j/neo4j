/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

//import org.scalatest.Ignore
import org.junit.Before

//import org.neo4j.graphdb.Node
//import org.junit.Assert._

class NullBehaviourTest extends ExecutionEngineHelper {

  /**
   *                    Currently (Should be)
   * CREATE null     => FAILS
   * DELETE null     => IGNORES
   *
   * More debatable
   *
   * SET null.x      => FAILS with MatchError (IGNORE)
   * REMOVE null.x   => IGNORES
   *
   * SET null:Foo    => FAILS with NullPointerException FAIL (IGNORE)
   * REMOVE null:Foo => FAILS with NullPointerException IGNORE (IGNORE)
   */

  @Before
  def delete_all_data() {
    deleteAllEntities()
  }

//  @Test
//  def should_fail_setting_properties_on_unbound_nodes() {
//    // given
//    createLabeledNode("Person")
//
//    // when
//    try {
//      parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m.key1 = 'value1' RETURN m" )
//      fail("Expected exception")
//    } catch {
//      case (_: Exception) => // expected
//    }
//  }
//
//  @Test
//  def should_ignore_setting_property_to_null_on_unbound_nodes() {
//    // given
//    createLabeledNode("Person")
//
//    // when
//    try {
//      parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m.key1 = null RETURN m" )
//      fail("Expected exception")
//    } catch {
//      case (_: Exception) => // expected
//    }
//
//    // then
//    // assert( List(null) === result.columnAs[Node]("m").toList)
//  }
//
//  @Test
//  def should_compare_unbound_as_equal_in_return() {
//    // given
//    createLabeledNode("Person")
//
//    // when
//    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m = m AS result" )
//
//    // then
//    val next: Boolean = result.columnAs[Boolean]("result").next()
//    assertTrue( "unbound should be equal to itself", next )
//  }
//
//  @Test
//  def with_should_turn_optional_nodes_into_null() {
//    /*
//    d:Person
//    a:Person->d
//    a:Person->b:Animal
//
//     */
//    // given
//    val a = createLabeledNode("Person")
//    val b = createLabeledNode("Animal")
//    val c = createLabeledNode("Person")
//    val d = createLabeledNode("Person")
//
//    relate(a, b)
//    relate(a, c)
//
//    // when
//    val result = parseAndExecute("MATCH n:Person-[r?]->(m) WITH n, m WHERE m:Animal RETURN n, m" ).toSet
//
//    // then
//    assert( Set(Map("n" -> a, "m" -> b)) === result )
//  }
//
//  @Test
//  def should_ignore_removing_properties_on_unbound_nodes() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) REMOVE m.key1 RETURN m" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("m").toList)
//  }
//
//  @Test
//  def should_ignore_setting_labels_on_unbound_nodes() {
//    // given
//    val a = createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m:Animal RETURN m" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("m").toList)
//    assert( Set("Person") == labels(a) )
//  }
//
//  @Test
//  def should_ignore_removing_labels_from_unbound_nodes() {
//    // given
//    val a = createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) REMOVE m:Person RETURN m" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("m").toList)
//    assert( Set("Person") == labels(a) )
//  }
//
//  @Test
//  def should_treat_unbound_nodes_as_null_in_create() {
//    // given
//    createLabeledNode("Person")
//
//    // when
//    try {
//      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE (m)-[result:KNOWS]->() RETURN result" )
//      fail("Expected exception")
//    }
//    catch {
//      case (_: IllegalArgumentException) => // we need to figure out the actual type
//    }
//
//    // check we didn't create extra nodes
//    assert( 1 === countNodes() )
//  }
//
//  @Test
//  def should_treat_unbound_nodes_as_null_in_foreach() {
//    // given
//    val a = createLabeledNode("Person")
//
//    // when
//    try {
//      parseAndExecute("START n=node(*) MATCH n-[r?:KNOWS]->(m) FOREACH( l in [m] | CREATE (l)-[:EXTRA]->() ) RETURN n" )
//      fail("Expected exception")
//    }
//    catch {
//      case (_: IllegalArgumentException) => // we need to figure out the actual type
//    }
//
//    // check we didn't create extra nodes
//    assert( 1 === countNodes() )
//  }
//
//  @Test
//  def should_treat_unbound_nodes_as_null_in_create_unique() {
//    // given
//    createLabeledNode("Person")
//
//
//    // when
//    try {
//      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE UNIQUE (m)-[result:KNOWS]->() RETURN result" )
//      fail("Expected exception")
//    }
//    catch {
//      case (_: IllegalArgumentException) => // we need to figure out the actual type
//    }
//
//    // check we didn't create extra nodes
//    assert( 1 === countNodes() )
//  }
//
//  @Test
//  def should_treat_unbound_nodes_as_null_in_delete() {
//    // given
//    val a = createLabeledNode("Person")
//
//    // when
//    val result =
//      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) DELETE n, m RETURN n, r, m" )
//
//    // then
//    assert( List(Map("n" -> a, "r" -> null, "m" -> null)) === result.toList )
//
//    // check we actually deleted the node
//    assert( 0 === countNodes() )
//  }
//
//  @Test
//  def should_treat_unbound_nodes_as_null_in_coalesce() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN COALESCE(m, n.key1) AS result" )
//
//    // then
//    assert( List("value1") === result.columnAs[Node]("result").toList)
//  }
//
//  @Test
//  def should_reveal_unbound_value_through_str_function() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]-(m) RETURN STR(m) = STR(null) as result" )
//
//    // then
//    assert( List(true) === result.columnAs[Boolean]("result").toList)
//  }
//
//  @Test
//  def should_give_null_for_relationship_start() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("START n=node(*) MATCH (n:Person)<-[r?]-(m) RETURN STARTNODE(r) as result" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("result").toList)
//  }
//
//  @Test
//  def should_give_null_for_relationship_end() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("START n=node(*) MATCH (n:Person)<-[r?]-(m) RETURN ENDNODE(r) as result" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("result").toList)
//  }
//
//
//  @Test
//  def should_ignore_using_unbound_nodes_in_merge_on_match() {
//    // given
//    val a = createNode()
//    val b = createLabeledNode("Person")
//
//    // when
//    try {
//      // when
//      parseAndExecute(s"START n=node(${nodeId(a)}) MATCH n-[r?]->(m) MERGE (p:Person) ON MATCH p SET m.age = 35 RETURN m, p" )
//      fail("Expected exception")
//    }
//    catch {
//      case (_: IllegalArgumentException) => // we need to figure out the actual type
//    }
//  }
//
//  @Test
//  def should_ignore_using_unbound_nodes_in_merge_on_create() {
//    // given
//    val a = createNode()
//
//    // when
//    val result = parseAndExecute(s"START n=node(${nodeId(a)}) MATCH n-[r?]->(m) MERGE (p:Person) ON CREATE p SET m.age = 35 RETURN m" )
//
//    // then
//    assert( List(Map("m" -> null)) === result.toList )
//  }

//  @Test
//  def should_return_null_when_getting_properties_from_unbound_nodes() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN m.key1" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("m.key1").toList)
//  }

//  @Test
//  def labels_of_unbound_nodes_should_equal_labels_of_null() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN labels(m) = labels(null) AS result" )
//
//    // then
//    assert( List(true) === result.columnAs[Boolean]("result").toList)
//  }
}
