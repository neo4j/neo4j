/*
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
package org.neo4j.internal.cypher.acceptance

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
    aNode = createLabeledNode(Map("name" -> "ABCDEF"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab"), "LABEL")
    eNode = createLabeledNode(Map("name" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }

  // *** TESTS OF PREFIX SEARCH

  test("should be case sensitive") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Location")
      }
    }
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%' RETURN l"

    val result = executeWithCostPlannerOnly(query)

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("l" -> london)))
  }

  test("should only match on the actual prefix") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    graph.inTx {
      createLabeledNode(Map("name" -> "Johannesburg"), "Location")
      createLabeledNode(Map("name" -> "Paris"), "Location")
      createLabeledNode(Map("name" -> "Malmo"), "Location")
      createLabeledNode(Map("name" -> "Loondon"), "Location")
      createLabeledNode(Map("name" -> "Lolndon"), "Location")
      (1 to 300).foreach { _ =>
        createLabeledNode("Location")
      }
    }
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%' RETURN l"

    val result = executeWithCostPlannerOnly(query)

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("l" -> london)))
  }

  test("should plan the leaf with the longest prefix if multiple LIKE patterns") {
    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'w%' AND a.prop LIKE 'www%' RETURN a")

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.executionPlanDescription().toString should include("prop LIKE www%")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should plan an IndexRangeSeek for a % string prefix search when index exists") {
    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'www%' RETURN a")

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should plan an IndexRangeSeek for a _ string prefix search when index exists") {

    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'ww_' RETURN a")

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("a" -> a2)))
  }

  test("should plan an IndexRangeSeek for a string search that starts with a prefix when index exists") {

    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'ww%w%' RETURN a")

    result.executionPlanDescription().toString should include("NodeIndexRangeSeek")
    result.executionPlanDescription().toString should include("Filter")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }


  test("should plan a UniqueIndexSeek when constraint exists") {

    graph.inTx {
      (1 to 300).foreach { _ =>
        createLabeledNode("Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createConstraint("Address", "prop")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'www%' RETURN a")

    result.executionPlanDescription().toString should include("NodeUniqueIndexRangeSeek")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should not plan an IndexSeek when index doesn't exist") {

    (1 to 10).foreach { _ =>
      createLabeledNode("Address")
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'www%' RETURN a")

    result.executionPlanDescription().toString should not include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should not plan an IndexSeek when the LIKE pattern does not have a valid prefix") {

    (1 to 10).foreach { _ =>
      createLabeledNode("Address")
    }

    graph.createIndex("Address", "prop")
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    val result = executeWithCostPlannerOnly("MATCH (a:Address) WHERE a.prop LIKE 'ww_%' RETURN a")

    result.executionPlanDescription().toString should not include("NodeIndexRangeSeek")
    result.toList should equal(List(Map("a" -> a1), Map("a" -> a2)))
  }

  test("finds exact matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'ABCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("finds case insensitive matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ILIKE 'ABCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("start of string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'ABC%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("end of string1") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '%DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("case insensitive start of string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ILIKE 'ABC%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("case insensitive end of string1") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ILIKE '%DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> cNode)))
  }

  test("end of string2") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '%AB' RETURN a")

    result.toList should equal(Seq(Map("a" -> bNode)))
  }

  test("middle of string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'a%f' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("all the things") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '%' RETURN a")

    result.toList should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode)))
  }

  test("middle of string is known") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '%CD%' RETURN a")

    result.toList should equal(Seq(
      Map("a" -> aNode))
    )
  }

  test("LIKE against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE NULL RETURN a")

    result.toList shouldBe empty
  }


  test("NOT LIKE against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name NOT LIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  test("ILIKE against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ILIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  test("NOT ILIKE against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name NOT ILIKE NULL RETURN a")

    result.toList shouldBe empty
  }

  // *** TESTS OF _

  test("_ should match exactly one character") {
    val a1 = createNode(Map("prop" -> "value"))
    createNode(Map("prop" -> "valu"))

    val result = executeWithAllPlanners("MATCH a WHERE a.prop LIKE 'valu_' RETURN a")

    result.toList should equal(Seq(Map("a" -> a1)))
  }

  test("one letter at the start of a string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '_BCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("one letter at the end of a string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'ABCDE_' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("first and last letter are not known") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '_bcde_' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("single letter in the middle of a string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'AB_DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("underscore must match at least one letter") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'ABC_DEF' RETURN a")

    result.toList shouldBe empty
  }

  test("none of the things") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE '_' RETURN a")

    result.toList shouldBe empty
  }

  // *** TESTS OF FEATURE INTERACTION

  test("combining underscore and percent") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name LIKE 'A%C_EF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  // *** TESTS OF NOT

  test("NOT can be used infix for LIKE") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name NOT LIKE '%b%' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> bNode), Map("a" -> eNode)))
  }

  test("NOT can be used infix for ILIKE") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name NOT ILIKE '%b%' RETURN a")

    result.toList should equal(Seq(Map("a" -> eNode)))
  }
}
