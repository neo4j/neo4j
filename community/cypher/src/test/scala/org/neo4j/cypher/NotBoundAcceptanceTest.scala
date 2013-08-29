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

class NotBoundAcceptanceTest extends ExecutionEngineHelper {

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
  def label_predicates_over_unbound_nodes_in_match_should_be_ignored() {
    // given
    val a = createLabeledNode("Person")
    val b = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m:Person) RETURN n, m" ).toSet

    // then
    assert( Set(Map("n" -> a, "m" -> null), Map("n" -> b, "m" -> null)) === result )
  }

  @Test
  def predicates_over_unbound_nodes_in_where_should_be_ignored() {
    // given
    val a = createLabeledNode("Person")

    // when
    val result = parseAndExecute("START n=node(*) MATCH (n:Person)-[r?]->(m) WHERE m:Person RETURN n, m" ).toSet

    // then
    assert( Set(Map("n" -> a, "m" -> null)) === result )
  }
}

