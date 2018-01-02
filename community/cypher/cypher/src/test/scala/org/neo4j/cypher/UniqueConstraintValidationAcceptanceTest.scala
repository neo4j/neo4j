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
package org.neo4j.cypher

import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport

class UniqueConstraintValidationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with  CollectionSupport {

  test("should_enforce_uniqueness_constraint_on_create_node_with_label_and_property") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node:Label1 { key1:'value1' } )")

    // WHEN
    try {
      execute("create ( node:Label1 { key1:'value1' } )")

      fail("should have thrown exception")
    }
    catch
    {
      case e: CypherExecutionException =>
        assertThat(e.getMessage, containsString( "\"key1\"=[value1]" ))
    }
  }

  test("should_enforce_uniqueness_constraint_on_set_property") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node1:Label1 { seq: 1, key1:'value1' } ), ( node2:Label1 { seq: 2 } )")

    // WHEN
    try {
      execute("match (node2:Label1) where node2.seq = 2 set node2.key1 = 'value1'")

      fail("should have thrown exception")
    }
    catch
    {
      case e: CypherExecutionException =>
        assertThat(e.getMessage, containsString( "\"key1\"=[value1]" ))
    }
  }

  test("should_enforce_uniqueness_constraint_on_add_label") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node1:Label1 { seq: 1, key1:'value1' } ), ( node2 { seq: 2, key1:'value1' } )")

    // WHEN
    try {
      execute("match node2 where node2.seq = 2 set node2:Label1")

      fail("should have thrown exception")
    }
    catch
    {
      case e: CypherExecutionException =>
        assertThat(e.getMessage, containsString( "\"key1\"=[value1]" ))
    }
  }

  test("should_enforce_uniqueness_constraint_on_conflicting_data_in_same_statement") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")

    // WHEN
    try {
      execute("create ( node1:Label1 { key1:'value1' } ), ( node2:Label1 { key1:'value1' } )")

      fail("should have thrown exception")
    }
    catch
    {
      case e: CypherExecutionException =>
        assertThat(e.getMessage, containsString( "\"key1\"=[value1]" ))
    }
  }

  test("should_allow_remove_and_add_conflicting_data_in_one_statement") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node:Label1 { seq:1, key1:'value1' } )")

    var seq = 2
    for (resolve <- List("delete toRemove", "remove toRemove.key1", "remove toRemove:Label1", "set toRemove.key1 = 'value2'"))
    {
      // WHEN
      val q = "match (toRemove:Label1 {key1:'value1'}) " +
        resolve +
        " create ( toAdd:Label1 { seq: {seq}, key1: 'value1' } )"

      try {
        execute(q, "seq" -> seq)
      } catch {
        case e: Throwable => throw new RuntimeException(q, e)
      }

      // THEN
      val result = execute("match (n:Label1) where n.key1 = 'value1' return n.seq as seq")
      result.columnAs[Int]("seq").toList should equal(List(seq))
      seq += 1
    }
  }

  test("should_allow_creation_of_non_conflicting_data") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node:Label1 { key1:'value1' } )")

    // WHEN
    execute("create ( node { key1:'value1' } )")
    execute("create ( node:Label2 { key1:'value1' } )")
    execute("create ( node:Label1 { key1:'value2' } )")
    execute("create ( node:Label1 { key2:'value1' } )")

    // THEN
    val result = execute("match (n) where id(n) <> 0 return count(*) as nodeCount")
    result.columnAs[Int]("nodeCount").toList should equal(List(4))
  }
}
