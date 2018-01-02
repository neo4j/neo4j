/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class UniqueConstraintValidationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with ListSupport {

  test("should enforce uniqueness constraint on create node with label and property") {
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
        assertThat(e.getMessage, containsString( "`key1` = 'value1'" ))
    }
  }

  test("should enforce uniqueness constraint on set property") {
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
        assertThat(e.getMessage, containsString( "`key1` = 'value1'" ))
    }
  }

  test("should enforce uniqueness constraint on add label") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is unique")
    execute("create ( node1:Label1 { seq: 1, key1:'value1' } ), ( node2 { seq: 2, key1:'value1' } )")

    // WHEN
    try {
      execute("match (node2) where node2.seq = 2 set node2:Label1")

      fail("should have thrown exception")
    }
    catch
    {
      case e: CypherExecutionException =>
        assertThat(e.getMessage, containsString( "`key1` = 'value1'" ))
    }
  }

  test("should enforce uniqueness constraint on conflicting data in same statement") {
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
        assertThat(e.getMessage, containsString( "`key1` = 'value1'" ))
    }
  }

  test("should allow remove and add conflicting data in one statement") {
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

  test("should allow creation of non conflicting data") {
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
