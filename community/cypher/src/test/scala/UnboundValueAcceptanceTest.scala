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
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m" )

    // then
    assert( null.asInstanceOf[Node] === result.columnAs[Node]("m").next())
  }

  @Test
  def should_return_unbound_relationship_as_null() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN r" )

    // then
    assert( null.asInstanceOf[Relationship] === result.columnAs[Relationship]("r").next())
  }

  @Test
  def should_not_fail_due_to_label_test_on_unbound() {
    // given
    val a = createLabeledNode("Person")

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
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) RETURN m = m AS result" )

    // then
    val next: Boolean = result.columnAs[Boolean]("result").next()
    assertFalse( "unbound should not be equal to itself", next )
  }

  @Test
  def should_compare_two_different_unbounds_as_non_equal() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m), (n)-[l?]->(k) RETURN m = k AS result" )

    // then
    val next: Boolean = result.columnAs[Boolean]("result").next()
    assertFalse( "unbound values should not be equal", next )
  }

  @Test
  def should_compare_unbound_and_null_as_equal() {
    // given
    val a = createLabeledNode("Person")

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
  def should_handle_labels_on_optional_nodes_in_with() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("MATCH (n:Person) WITH n MATCH n-[r?]->(m:Person) RETURN n, m" )

    // then
    assert( Set(a) === result.columnAs[Node]("n").toSet)
  }
}