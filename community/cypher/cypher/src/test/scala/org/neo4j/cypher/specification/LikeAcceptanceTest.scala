/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.specification

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node

class LikeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null
  var dNode: Node = null
  var eNode: Node = null
  var fNode: Node = null

  override def initTest() {
    super.initTest()
    aNode = createLabeledNode(Map("name" -> "ABCDEF", "value" -> "42%"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB", "value" -> "42_"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef", "value" -> "[42]"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab", "value" -> "42"), "LABEL")
    eNode = createLabeledNode(Map("name" -> "", "value" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }


  // ***********************  TESTS OF %
  test("finds exact matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'ABCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("finds case insensitive matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name ILIKE 'ABCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("start of string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'ABC%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("end of string1") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '%DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("case insensitive start of string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name ILIKE 'ABC%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("case insensitive end of string1") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name ILIKE '%DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("end of string2") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '%AB' RETURN a")

    result.toList should equal(Seq(Map("a" -> bNode)))
  }

  test("middle of string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'a%f' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("all the things") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '%' RETURN a")

    result.toList should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode)))
  }

  test("middle of string is known") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '%CD%' RETURN a")

    result.toList should equal(Seq(
      Map("a" -> aNode))
    )
  }

  test("LIKE against a null value returns no matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE NULL RETURN a")

    result.toList shouldBe empty
  }


  test("NOT LIKE against a null value returns no matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name NOT LIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  test("ILIKE against a null value returns no matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name ILIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  test("NOT ILIKE against a null value returns no matches") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name NOT ILIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  // ***********************  TESTS OF _
  test("one letter at the start of a string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '_BCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("one letter at the end of a string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'ABCDE_' RETURN a")

    result.toList should equal(Seq((Map("a" -> aNode))))
  }

  test("first and last letter are not known") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '_bcde_' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("single letter in the middle of a string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'AB_DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("underscore must match at least one letter") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'ABC_DEF' RETURN a")

    result.toList shouldBe empty
  }

  test("none of the things") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '_' RETURN a")

    result.toList shouldBe empty
  }

  // ***********************  TESTS OF []
  test("option at the beginning of the string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '[Aa]BCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("non-matching at the beginning of the string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '[XZ]BCDEF' RETURN a")

    result.toList shouldBe empty
  }

  test("option at the end of the string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'abcde[Xf]' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("non-matching option at the end of the string") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'abcde[XY]' RETURN a")

    result.toList shouldBe empty
  }

  // ***********************  ESCAPE testing
  test("match on percent character") {
    val result = executeWithNewPlanner("""MATCH (a) WHERE a.value LIKE '%\\%' ESCAPE '\\' RETURN a""")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("match on underscore character") {
    val result = executeWithNewPlanner("""MATCH (a) WHERE a.value LIKE '%\\_' ESCAPE '\\' RETURN a""")

    result.toList should equal(Seq(Map("a" -> bNode)))
  }

  test("match on [] character") {
    val result = executeWithNewPlanner("""MATCH (a) WHERE a.value LIKE '^[%^]' ESCAPE '^' RETURN a""")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  // ***********************  CO-CO-CO-COMBO
  test("combining underscore and percent") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE 'A%C_EF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("combining percent and option") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name LIKE '[Aa]%' RETURN a")

    result.toList should equal(Seq(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode)
    )
    )
  }

  test("NOT can be used infix for LIKE") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name NOT LIKE '%b%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> bNode), Map("a" -> eNode)))
  }

  test("NOT can be used infix for ILIKE") {
    val result = executeWithNewPlanner("MATCH (a) WHERE a.name NOT ILIKE '%b%' RETURN a")

    result.toList should equal(Seq(Map("a" -> eNode)))
  }
}
