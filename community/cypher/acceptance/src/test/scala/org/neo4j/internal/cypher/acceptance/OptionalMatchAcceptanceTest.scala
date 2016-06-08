/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.graphdb._

class OptionalMatchAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  var nodeA: Node = null
  var nodeB: Node = null
  var nodeC: Node = null
  var selfRel: Relationship = null

  override protected def initTest() = {
    super.initTest()
    val single = createLabeledNode(Map.empty[String, Any], "Single")
    nodeA = createLabeledNode(Map("prop" -> 42), "A")
    nodeB = createLabeledNode(Map("prop" -> 46), "B")
    nodeC = createLabeledNode(Map.empty[String, Any], "C")
    relate(single, nodeA)
    relate(single, nodeB)
    relate(nodeA, nodeC)
    selfRel = relate(nodeB, nodeB)
  }

  // TCK'd
  test("optional nodes with labels in match clause should return null when there is no match") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m:NonExistent) RETURN r")
    assert(result.toList === List(Map("r" -> null)))
  }

  // TCK'd
  test("optional nodes with labels in where clause should return null when there is no match") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m:NonExistent RETURN r")
    assert(result.toList === List(Map("r" -> null)))
  }

  // TCK'd
  test("predicates on optional matches should be respected") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m.prop = 42 RETURN m")
    assert(result.toList === List(Map("m" -> nodeA)))
  }

  // TCK'd
  test("has label on null should evaluate to null") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Single) OPTIONAL MATCH (n)-[r:TYPE]-(m) RETURN m:TYPE")
    assert(result.toList === List(Map("m:TYPE" -> null)))
  }

  // TCK'd
  test("should allow match following optional match if there is an intervening WITH when there are no results") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:NonExistent) OPTIONAL MATCH (a)-->(c:NonExistent) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d")
    assert(result.toList === List())
  }

  // TCK'd
  test("should allow match following optional match if there is an intervening WITH when there are results") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:A) OPTIONAL MATCH (a)-->(c:B) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d")
    assert(result.toList === List(Map("d" -> nodeC)))
  }

  // TCK'd
  test("should support optional match without any external dependencies in WITH") {
    val result = executeWithAllPlannersAndCompatibilityMode("OPTIONAL MATCH (a:A) WITH a AS a MATCH (b:B) RETURN a, b")

    assert(result.toList === List(Map("a" -> nodeA, "b" -> nodeB)))
  }

  // TCK'd
  test("should support named paths inside of optional matches") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A) OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p")

    assert(result.toList === List(Map("p" -> null)))
  }

  // TCK'd
  test("optional matching between two found nodes behaves as expected") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      """MATCH (a:A), (b:C)
        |OPTIONAL MATCH (x)-->(b)
        |RETURN x""".stripMargin)

    assert(result.toSet === Set(Map("x" -> nodeA)))
  }

  // TCK'd
  test("optional match with labels on the optional end node") {
    createLabeledNode("X")
    val x2 = createLabeledNode("X")
    val b1 = createLabeledNode("Y")
    val b2 = createLabeledNode("Y", "Z")
    relate(x2, b1)
    relate(x2, b2)

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a:X) OPTIONAL MATCH (a)-->(b:Y) RETURN b")

    result.toSet should equal(Set(Map("b" -> null), Map("b" -> b1), Map("b" -> b2)))
  }

  // TCK'd
  test("should support names paths inside of option matches with node predicates") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (b:B) OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p")

    assert(result.toList === List(Map("p" -> null)))
  }

  // TCK'd
  test("should support varlength optional relationships") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single) OPTIONAL MATCH (a)-[*]->(b) RETURN b")

    assert(result.toSet === Set(
      Map("b" -> nodeA),
      Map("b" -> nodeB),
      Map("b" -> nodeC)
    ))
  }

  // TCK'd
  test("should support varlength optional relationships that is longer than the existing longest") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single) OPTIONAL MATCH (a)-[*3..]-(b) RETURN b")

    assert(result.toSet === Set(Map("b" -> null)))
  }

  // TCK'd
  test("should support optional match to find self loops") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a:B) OPTIONAL MATCH (a)-[r]-(a) RETURN r")

    assert(result.toSet === Set(Map("r" -> selfRel)))
  }

  // TCK'd
  test("should support optional match to not find self loops") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) WHERE NOT (a:B) OPTIONAL MATCH (a)-[r]->(a) RETURN r")

    assert(result.toSet === Set(Map("r" -> null)))
  }

  // TCK'd
  test("should support varlength optional relationships where both ends are already bound") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single), (x:C) OPTIONAL MATCH (a)-[*]->(x) RETURN x")

    assert(result.toSet === Set(
      Map("x" -> nodeC)
    ))
  }

  // TCK'd
  test("should support varlength optional relationships where both ends are already bound but no paths exist") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (b:B) OPTIONAL MATCH p = (a)-[*]->(b) RETURN p")

    assert(result.toSet === Set(
      Map("p" -> null)
    ))
  }

  // TCK'd
  test("should support optional relationships where both ends are already bound") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b")

    assert(result.toSet === Set(
      Map("b" -> nodeA)
    ))
  }

  // TCK'd
  test("should support optional relationships where both ends are already bound and no paths exist") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b")

    assert(result.toSet === Set(
      Map("b" -> null)
    ))
  }

  // TCK'd
  test("should handle pattern predicates in optional match") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b) WHERE (b)-->(c) RETURN b")

    assert(result.toSet === Set(
      Map("b" -> null)
    ))
  }

  // TCK'd
  test("should handle pattern predicates in optional match with hit") {
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:Single), (c:C) OPTIONAL MATCH (a)-->(b) WHERE (b)-->(c) RETURN b")

    assert(result.toSet === Set(
      Map("b" -> nodeA)
    ))
  }

  // TCK'd
  test("should handle correlated optional matches - the first does not match, and the second must not match") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      """MATCH (a:A), (b:B)
        |OPTIONAL MATCH (a)-->(x)
        |OPTIONAL MATCH (x)-[r]->(b)
        |RETURN x, r""".stripMargin)

    assert(result.toSet === Set(
      Map("x" -> nodeC, "r" -> null)
    ))
  }

  // TCK'd
  test("should handle optional match between optionally matched things") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      """OPTIONAL MATCH (a:NotThere)
        |WITH a
        |MATCH (b:B)
        |WITH a, b
        |OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
        |RETURN a, b, r""".stripMargin)

    assert(result.toList === List(Map("b" -> nodeB, "r" -> null, "a" -> null)))
  }

  // TCK'd
  test("should handle optional match between nulls") {
    val result = executeWithAllPlannersAndCompatibilityMode(
      """OPTIONAL MATCH (a:NotThere)
        |OPTIONAL MATCH (b:NotThere)
        |WITH a, b
        |OPTIONAL MATCH (b)-[r:NOR_THIS]->(a)
        |RETURN a, b, r""".stripMargin)

    assert(result.toList === List(Map("b" -> null, "r" -> null, "a" -> null)))
  }

  // TCK'd
  test("optional match and collect should work") {
    createLabeledNode(Map("property" -> 42), "DoesExist")
    createLabeledNode(Map("property" -> 43), "DoesExist")
    createLabeledNode(Map("property" -> 44), "DoesExist")

    val query = """OPTIONAL MATCH (f:DoesExist)
                  |OPTIONAL MATCH (n:DoesNotExist)
                  |RETURN collect(DISTINCT n.property) AS a, collect(DISTINCT f.property) AS b""".stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("b" -> List(42, 43, 44), "a" -> List.empty)))
  }
}
