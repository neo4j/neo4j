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
import org.junit.Test

class UnboundValueAcceptanceTest extends ExecutionEngineHelper {

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

//  @Test
//  def should_return_true_for_any_has_relationship_predicate() {
//    // given
//    createLabeledNode(Map("key1" -> "value1"), "Person")
//
//    // when
//    val result = parseAndExecute("START n=node(0) MATCH (n)<-[r?]-(m) WHERE (m --> ()) RETURN m as result" )
//
//    // then
//    assert( List(null) === result.columnAs[Node]("result").toList)
//  }


}