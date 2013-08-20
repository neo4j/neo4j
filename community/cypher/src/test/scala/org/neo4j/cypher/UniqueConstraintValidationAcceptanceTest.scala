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

import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.neo4j.kernel.impl.api.constraints.ConstraintViolationKernelException

class UniqueConstraintValidationAcceptanceTest
  extends ExecutionEngineHelper with StatisticsChecker with Assertions with CollectionSupport {

  @Test
  def should_enforce_uniqueness_constraint_on_create_node_with_label_and_property() {
    // GIVEN
    parseAndExecute("create constraint on (node:Label1) assert node.key1 is unique")
    parseAndExecute("create ( node:Label1 { key1:'value1' } )")

    // WHEN
    try {
      parseAndExecute("create ( node:Label1 { key1:'value1' } )")

      fail("should have thrown exception")
    }
    catch
    {
      case e: ConstraintViolationKernelException =>
        assertThat(e.getMessage, containsString("already exists"))
    }
  }

  @Test
  def should_enforce_uniqueness_constraint_on_set_property() {
    // GIVEN
    parseAndExecute("create constraint on (node:Label1) assert node.key1 is unique")
    parseAndExecute("create ( node1:Label1 { seq: 1, key1:'value1' } ), ( node2:Label1 { seq: 2 } )")

    // WHEN
    try {
      parseAndExecute("match node2:Label1 where node2.seq = 2 set node2.key1 = 'value1'")

      fail("should have thrown exception")
    }
    catch
    {
      case e: ConstraintViolationKernelException =>
        assertThat(e.getMessage, containsString("already exists"))
    }
  }

  @Test
  def should_enforce_uniqueness_constraint_on_add_label() {
    // GIVEN
    parseAndExecute("create constraint on (node:Label1) assert node.key1 is unique")
    parseAndExecute("create ( node1:Label1 { seq: 1, key1:'value1' } ), ( node2 { seq: 2, key1:'value1' } )")

    // WHEN
    try {
      parseAndExecute("match node2 where node2.seq = 2 set node2:Label1")

      fail("should have thrown exception")
    }
    catch
    {
      case e: ConstraintViolationKernelException =>
        assertThat(e.getMessage, containsString("already exists"))
    }
  }

  @Test
  def should_enforce_uniqueness_constraint_on_conflicting_data_in_same_statement() {
    // GIVEN
    parseAndExecute("create constraint on (node:Label1) assert node.key1 is unique")

    // WHEN
    try {
      parseAndExecute("create ( node1:Label1 { key1:'value1' } ), ( node2:Label1 { key1:'value1' } )")

      fail("should have thrown exception")
    }
    catch
    {
      case e: ConstraintViolationKernelException =>
        assertThat(e.getMessage, containsString("already exists"))
    }
  }

  @Test
  def should_allow_remove_and_add_conflicting_data_in_one_statement() {
    // GIVEN
    parseAndExecute("create constraint on (node:Label1) assert node.key1 is unique")
    parseAndExecute("create ( node:Label1 { seq:1, key1:'value1' } )")

    // WHEN
    parseAndExecute("match toRemove:Label1 where toRemove.key1 = 'value1' delete toRemove create ( toAdd:Label1 { seq: 2, key1: 'value1' } )")

    // THEN
    val result: ExecutionResult = parseAndExecute("match n:Label1 where n.key1 = 'value1' return n.seq as seq")
    assertEquals(List(2), result.columnAs[Int]("seq").toList)
  }
}