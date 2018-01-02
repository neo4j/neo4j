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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.pipes.IndexSeekByRange
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.{Node, ResourceIterator}

class StartsWithAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

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
  test("should not plan an IndexSeek when index doesn't exist") {

    (1 to 10).foreach { _ =>
      createLabeledNode("Address")
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a")

    result should (not(use(IndexSeekByRange.name)) and evaluateTo(List(Map("a" -> a1), Map("a" -> a2))))
  }

  test("finds exact matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH 'ABCDEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("start of string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH 'ABC' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("end of string1") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ENDS WITH 'DEF' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  test("end of string2") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ENDS WITH 'AB' RETURN a")

    result.toList should equal(Seq(Map("a" -> bNode)))
  }

  test("middle of string") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH 'a' and a.name ENDS WITH 'f' RETURN a")

    result.toList should equal(Seq(Map("a" -> cNode)))
  }

  test("all the things") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH '' RETURN a")

    result.toList should equal(List(
      Map("a" -> aNode),
      Map("a" -> bNode),
      Map("a" -> cNode),
      Map("a" -> dNode),
      Map("a" -> eNode)))
  }

  test("middle of string is known") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name CONTAINS 'CD' RETURN a")

    result.toList should equal(Seq(
      Map("a" -> aNode))
    )
  }

  test("STARTS WITH against a whitespace should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH ' ' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> " Mats ")))
  }

  test("STARTS WITH against a newline should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH '\n' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> "\nMats\n")))
  }

  test("ENDS WITH against a whitespace should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ENDS WITH ' ' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> " Mats ")))
  }

  test("ENDS WITH against a newline should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ENDS WITH '\n' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> "\nMats\n")))
  }

  test("CONTAINS against a whitespace should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name CONTAINS ' ' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> " Mats ")))
  }

  test("CONTAINS against a newline should work") {
    createNode(Map("name" -> " Mats "))
    createNode(Map("name" -> "\nMats\n"))
    createNode(Map("name" -> "\tMats\t"))

    val result = executeWithAllPlanners("MATCH (a) WHERE a.name CONTAINS '\n' RETURN a.name AS name")

    result.toSeq should equal(Seq(Map("name" -> "\nMats\n")))
  }

  test("STARTS WITH against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name STARTS WITH NULL RETURN a")

    result.toList shouldBe empty
  }


  test("NOT STARTS WITH against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE NOT a.name STARTS WITH NULL RETURN a")

    result.toList shouldBe empty
  }

  test("ENDS WITH against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name ENDS WITH NULL RETURN a")

    result.toList shouldBe empty
  }


  test("NOT ENDS WITH against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE NOT a.name ENDS WITH NULL RETURN a")

    result.toList shouldBe empty
  }

  test("CONTAINS against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE a.name CONTAINS NULL RETURN a")

    result.toList shouldBe empty
  }


  test("NOT CONTAINS against a null value returns no matches") {
    val result = executeWithAllPlanners("MATCH (a) WHERE NOT a.name CONTAINS NULL RETURN a")

    result.toList shouldBe empty
  }

  // *** TESTS OF FEATURE INTERACTION
  test("combining string operators") {
    val result = executeWithAllPlanners(
      """MATCH (a)
        |WHERE a.name STARTS WITH 'A'
        |  AND a.name CONTAINS 'C'
        |  AND a.name ENDS WITH 'EF'
        |RETURN a""".stripMargin)

    result.toList should equal(Seq(Map("a" -> aNode)))
  }

  // *** TESTS OF NOT
  test("NOT can be used when evaluating CONTAINS") {
    val result = executeWithAllPlanners("MATCH (a) WHERE NOT a.name CONTAINS 'b' RETURN a")

    result.toList should equal(Seq(Map("a" -> aNode), Map("a" -> bNode), Map("a" -> eNode)))
  }

  // *** TESTS OF TRANSACTION STATE HANDLING

  test("Should handle prefix search with existing transaction state") {
    graph.createIndex("User", "name")
    graph.inTx {
      createLabeledNode(Map("name" -> "Stefan"), "User")
      createLabeledNode(Map("name" -> "Stephan"), "User")
      createLabeledNode(Map("name" -> "Stefanie"), "User")
      createLabeledNode(Map("name" -> "Craig"), "User")
    }
    graph.inTx {
      drain(graph.execute("MATCH (u:User {name: 'Craig'}) SET u.name = 'Steven'"))
      drain(graph.execute("MATCH (u:User {name: 'Stephan'}) DELETE u"))
      drain(graph.execute("MATCH (u:User {name: 'Stefanie'}) SET u.name = 'steffi'"))

      val result = executeWithAllPlanners("MATCH (u:User) WHERE u.name STARTS WITH 'Ste' RETURN u.name as name").columnAs("name").toList.toSet

      result should equal(Set[String]("Stefan", "Steven"))
    }
  }

  private def drain(iter: ResourceIterator[_]): Unit = {
    try {
      while (iter.hasNext) iter.next()
    } finally {
      iter.close()
    }
  }
}
