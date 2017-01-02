/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ParameterValuesAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  // Not TCK material below; sending graph types or characters as parameters is not supported

  test("should be able to send in node via parameter") {
    // given
    val node = createLabeledNode("Person")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (b) WHERE b = {param} RETURN b", "param" -> node)
    result.toList should equal(List(Map("b" -> node)))
  }

  test("should be able to send in relationship via parameter") {
    // given
    val rel = relate(createLabeledNode("Person"), createLabeledNode("Person"))

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (:Person)-[r]->(:Person) WHERE r = {param} RETURN r", "param" -> rel)
    result.toList should equal(List(Map("r" -> rel)))
  }

  test("should treat chars as strings in equality") {
    executeScalar[Boolean]("RETURN 'a' = {param}", "param" -> 'a') shouldBe true
    executeScalar[Boolean]("RETURN {param} = 'a'", "param" -> 'a') shouldBe true
  }

  test("removing property when not sure if it is a node or relationship should still work - NODE") {
    val n = createNode("name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> n)

    graph.inTx {
      n.getProperty("lastname") should equal("Anders")
      n.hasProperty("name") should equal(false)
    }
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> r)

    graph.inTx {
      r.getProperty("lastname") should equal("Anders")
      r.hasProperty("name") should equal(false)
    }
  }
}
