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

import org.junit.Assert._
import org.neo4j.graphdb._
import org.junit.{Before, Test}

class UnboundValueAcceptanceTest extends ExecutionEngineHelper {

  @Before
  def delete_all_data() {
    deleteAllEntities()
  }

  @Test
  def should_return_unbound_node_as_null() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m" )

    // then
    assert( null.asInstanceOf[Node] === result.columnAs[Node]("m").next())
  }

  @Test
  def should_return_unbound_relationship_as_null() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN r" )

    // then
    assert( null.asInstanceOf[Relationship] === result.columnAs[Relationship]("r").next())
  }

  @Test
  def should_not_fail_due_to_label_test_on_unbound() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m:Person) RETURN m AS result" )

    // then
    val iterator = result.columnAs[Node]("result")
    assert( null.asInstanceOf[Node] === iterator.next() )
    assertFalse( "expected iterator to be exhausted", iterator.hasNext )
  }

  @Test
  def should_compare_unbound_as_non_equal() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m = m AS result" )

    // then
    val next: Boolean = result.columnAs[Boolean]("result").next()
    assertFalse( "unbound should not be equal to itself", next )
  }

  @Test
  def should_compare_two_different_unbounds_as_non_equal() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m), (n)-[l?]->(k) RETURN m = k AS result" )

    // then
    val next: Boolean = result.columnAs[Boolean]("result").next()
    assertFalse( "unbound values should not be equal", next )
  }

  @Test
  def should_compare_unbound_and_null_as_equal() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m = null AS result" )

    // then
    val next: Boolean = result.columnAs[Boolean]("result").next()
    assertTrue( "unbound should be equal to null", next )
  }

  @Test
  def should_handle_labels_on_optional_nodes() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m:Person) RETURN n, m" )

    // then
    assert( Set(a, b) === result.columnAs[Node]("n").toSet)
  }

  @Test
  def should_handle_where_clause_about_labels_on_optional_nodes() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) WHERE m:Person RETURN n, m" )

    // then
    assert( Set(a, b) === result.columnAs[Node]("n").toSet)
  }

  @Test
  def should_give_null_for_type_of_optional_relationship() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN type(r) AS result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_handle_labels_on_optional_nodes_in_with() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m:Person) RETURN n, m" )

    // then
    assert( Set(a) === result.columnAs[Node]("n").toSet)
  }

  @Test
  def should_return_null_when_getting_properties_from_unbound_nodes() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN m.key1" )

    // then
    assert( List(null) === result.columnAs[Node]("m.key1").toList)
  }

  @Test
  def should_return_empty_set_for_labels_of_unbound_nodes() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN labels(m) AS result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_ignore_setting_properties_on_unbound_nodes() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m.key1 = 'value1' RETURN m" )

    // then
    assert( List(null) === result.columnAs[Node]("m").toList)
  }

  @Test
  def should_ignore_setting_property_to_null_on_unbound_nodes() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m.key1 = null RETURN m" )

    // then
    assert( List(null) === result.columnAs[Node]("m").toList)
  }

  @Test
  def should_ignore_removing_properties_on_unbound_nodes() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) REMOVE m.key1 RETURN m" )

    // then
    assert( List(null) === result.columnAs[Node]("m").toList)
  }

  @Test
  def should_ignore_setting_labels_on_unbound_nodes() {
    // given
    val a = createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) SET m:Animal RETURN m" )

    // then
    assert( List(null) === result.columnAs[Node]("m").toList)
    assert( Set("Person") == labels(a) )
  }

  @Test
  def should_ignore_removing_labels_from_unbound_nodes() {
    // given
    val a = createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) REMOVE m:Person RETURN m" )

    // then
    assert( List(null) === result.columnAs[Node]("m").toList)
    assert( Set("Person") == labels(a) )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_create() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE (m)-[result:KNOWS]->() RETURN result" )
//    val result = parseAndExecute("START n=node(*) CREATE ()-[result:HAPPY]->() RETURN result" )


    // then
    assert( List(null) === result.columnAs[Node]("result").toList )

    // check we didn't create extra nodes
    assert( 1 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_create_but_not_bound_ones() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")
    createLabeledNode("Person")
    relate(a, b)

    // when
    val result = parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE (m)-[result:KNOWS]->() RETURN result" )

    // then
    assert( 3 === result.columnAs[Node]("result").toList.size )

    // check we create one extra node
    assert( 4 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_create_but_not_bound_ones_when_using_foreach() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")
    createLabeledNode("Person")
    relate(a, b, "KNOWS", "Heidi")

    // when
    val result =
      parseAndExecute("START n=node(*) MATCH n-[r?:KNOWS]->(m) FOREACH( l in [m] | CREATE (l)-[:EXTRA]->() ) RETURN n" )

    // then
    assert( 3 === result.columnAs[Node]("n").toList.size )

    // check we create one extra node
    assert( 4 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_create_unique() {
    // given
    createLabeledNode("Person")

    // when
    val result =
      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE UNIQUE (m)-[result:KNOWS]->() RETURN result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList )

    // check we didn't create extra nodes
    assert( 1 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_create_unique_but_not_bound_ones() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")
    createLabeledNode("Person")
    relate(a, b)

    // when
    val result =
      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) CREATE UNIQUE (m)-[result:KNOWS]->() RETURN result" )

    // then
    assert( 3 === result.columnAs[Node]("result").toList.size )

    // check we create one extra node
    assert( 4 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_delete() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result =
      parseAndExecute("START n=node(*) MATCH n-[r?]->(m) DELETE n, m RETURN n, r, m" )

    // then
    assert( List(Map("n" -> a, "r" -> null, "m" -> null)) === result.toList )

    // check we actually deleted the node
    assert( 0 === countNodes() )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_complex_delete() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")
    createLabeledNode("Person")
    val r = relate(a, b)

    // when
    val result =
      parseAndExecute(s"START n=node(${nodeId(a)}) MATCH n-[r?]->(m) DELETE n, r, m RETURN n, r, m" )

    // then
    assert( List(Map("n" -> a, "r" -> r, "m" -> b)) === result.toList )

    // check we actually deleted the node
    assert( 1 === countNodes() )
  }

  @Test
  def should_coalesce_unbound_values() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m) RETURN COALESCE(m, n.key1) AS result" )

    // then
    assert( List("value1") === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_reveal_unbound_value_through_str_function() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]-(m) RETURN STR(m) as result" )

    // then
    assert( List("UNBOUND_VALUE") === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_give_null_for_relationship_start() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)<-[r?]-(m) RETURN STARTNODE(r) as result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_give_null_for_relationship_end() {
    // given
    createLabeledNode(Map("key1" -> "value1"), "Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)<-[r?]-(m) RETURN ENDNODE(r) as result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_treat_unbound_as_null_in_is_null() {
    // given
    createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)<-[r?]-(m) WHERE m IS NULL RETURN m as result" )

    // then
    assert( List(null) === result.columnAs[Node]("result").toList)
  }

  @Test
  def should_ignore_using_unbound_nodes_in_merge_on_match() {
    // given
    val a = createNode()
    val b = createLabeledNode("Person")

    // when
    val result = parseAndExecute(s"START n=node(${nodeId(a)}) MATCH n-[r?]->(m) MERGE (p:Person) ON MATCH p SET m.age = 35 RETURN m, p" )

    // then
    assert( List(Map("m" -> null, "p" -> b)) === result.toList )
  }

  @Test
  def should_ignore_using_unbound_nodes_in_merge_on_create() {
    // given
    val a = createNode()

    // when
    val result = parseAndExecute(s"START n=node(${nodeId(a)}) MATCH n-[r?]->(m) MERGE (p:Person) ON CREATE p SET m.age = 35 RETURN m" )

    // then
    assert( List(Map("m" -> null)) === result.toList )
  }
}

