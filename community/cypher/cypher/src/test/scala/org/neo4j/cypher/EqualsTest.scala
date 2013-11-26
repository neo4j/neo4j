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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.junit.Test
import org.scalatest._
import org.neo4j.cypher.{IncomparableValuesException, ExecutionEngineHelper}

class EqualsTest extends ExecutionEngineHelper with Matchers {

  @Test
  def should_prohibit_equals_between_node_and_parameter() {
    // given
    createLabeledNode("Person")

    evaluating {
      execute("MATCH (b) WHERE b = {param} RETURN b", "param" -> Map("name" -> "John Silver"))
    } should produce[IncomparableValuesException]
  }

  @Test
  def should_prohibit_equals_between_parameter_and_node() {
    // given
    createLabeledNode("Person")

    evaluating {
      execute("MATCH (b) WHERE {param} = b RETURN b", "param" -> Map("name" -> "John Silver"))
    } should produce[IncomparableValuesException]
  }

  @Test
  def should_allow_equals_between_node_and_node() {
    // given
    createLabeledNode("Person")

    // when
    val result = executeScalar[Number]("MATCH (a) WITH a MATCH (b) WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }

  @Test
  def should_reject_equals_between_node_and_property() {
    // given
    createLabeledNode(Map("val"->17), "Person")

    evaluating {
      execute("MATCH (a) WHERE a = a.val RETURN count(a)")
    } should produce[IncomparableValuesException]
  }

  @Test
  def should_allow_equals_between_relationship_and_relationship() {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    // when
    val result = executeScalar[Number]("MATCH ()-[a]->() WITH a MATCH ()-[b]->() WHERE a = b RETURN count(b)")

    // then
    result should be (1)
  }


  @Test
  def should_reject_equals_between_node_and_relationship() {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    evaluating {
      execute("MATCH (a)-[b]->() RETURN a = b")
    } should produce[IncomparableValuesException]
  }

  @Test
  def should_reject_equals_between_relationship_and_node() {
    // given
    relate(createLabeledNode("Person"), createLabeledNode("Person"))

    evaluating {
      execute("MATCH (a)-[b]->() RETURN b = a")
    } should produce[IncomparableValuesException]
  }
}