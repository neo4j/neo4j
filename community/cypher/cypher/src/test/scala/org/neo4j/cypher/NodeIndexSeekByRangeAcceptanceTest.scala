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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{IndexSeekByRange, UniqueIndexSeekByRange}

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexSeekByRangeAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

  test("should handle comparing large integers") {
    // Given
    val person = createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWithCostPlannerOnly(
      "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486378 RETURN p")

    // Then
    result should (use("NodeIndexSeekByRange") and evaluateTo(List(Map("p" -> person))))
  }

  test("should handle comparing large integers 2") {
    // Given
    createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWithCostPlannerOnly(
      "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486379 RETURN p")

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  ignore("should be case sensitive for Like with indexes") { // Replace with a test using startsWith
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%' RETURN l"

    val result = executeWithAllPlanners(query)

    result should (use(IndexSeekByRange.name) and evaluateTo(List(Map("l" -> london))))
  }

  ignore("should perform prefix search in an update query") {// Replace with a test using startsWith
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%' CREATE (L:Location {name: toUpper(l.name)}) RETURN L.name AS NAME"

    val result = executeWithRulePlanner(query)

    result should (use("SchemaIndex", "PrefixSeekRange") and evaluateTo(List(Map("NAME" -> "LONDON"))))
  }

  ignore("should perform prefix search for _ in an update query") {// Replace with a test using startsWith
    createLabeledNode(Map("name" -> "Loony"), "Location")
    createLabeledNode(Map("name" -> "loony"), "Location")
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Loon_' CREATE (L:Location {name: toUpper(l.name)}) RETURN L.name AS NAME"

    val result = executeWithRulePlanner(query)

    result should (use("SchemaIndex", "PrefixSeekRange") and evaluateTo(List(Map("NAME" -> "LOONY"))))
  }

  ignore("should perform prefix search for _ in an update query with complex prefix") {// Replace with a test using startsWith
    createLabeledNode(Map("name" -> "Loonyboom"), "Location")
    createLabeledNode(Map("name" -> "loonyboom"), "Location")
    createLabeledNode(Map("name" -> "boom"), "Location")
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Loon_boom' CREATE (L:Location {name: toUpper(l.name)}) RETURN L.name AS NAME"

    val result = executeWithRulePlanner(query)

    result should (use("SchemaIndex", "PrefixSeekRange") and evaluateTo(List(Map("NAME" -> "LOONYBOOM"))))
  }

  ignore("should perform complex prefix search in an update query)") {// Replace with a test using startsWith
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "Londinium"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%don' CREATE (L:Location {name: toUpper(l.name)}) RETURN L.name AS NAME"

    val result = executeWithRulePlanner(query)

    result should (use("SchemaIndex", "PrefixSeekRange", "Filter") and evaluateTo(List(Map("NAME" -> "LONDON"))))
  }

  ignore("should only match on the actual prefix") {// Replace with a test using startsWith
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    graph.inTx {
      createLabeledNode(Map("name" -> "Johannesburg"), "Location")
      createLabeledNode(Map("name" -> "Paris"), "Location")
      createLabeledNode(Map("name" -> "Malmo"), "Location")
      createLabeledNode(Map("name" -> "Loondon"), "Location")
      createLabeledNode(Map("name" -> "Lolndon"), "Location")

      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }
    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name LIKE 'Lon%' RETURN l"

    val result = executeWithAllPlanners(query)

    result should (use(IndexSeekByRange.name) and evaluateTo(List(Map("l" -> london))))
  }

  ignore("should plan the leaf with the longest prefix if multiple LIKE patterns") {// Replace with a test using startsWith

    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Address")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("prop" -> i.toString), "Address")
      }
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop LIKE 'w%' AND a.prop LIKE 'www%' RETURN a")

    result should (use(IndexSeekByRange.name) and evaluateTo(List(Map("a" -> a1), Map("a" -> a2))))
    result.executionPlanDescription().toString should include("prop LIKE www%")
  }

  ignore("should plan an IndexRangeSeek for a % string prefix search when index exists") {// Replace with a test using startsWith
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Address")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("prop" -> i.toString), "Address")
      }
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop LIKE 'www%' RETURN a")

    result should (use(IndexSeekByRange.name) and evaluateTo(List(Map("a" -> a1), Map("a" -> a2))))
  }

  ignore("should plan an IndexRangeSeek for a _ string prefix search when index exists") {// Replace with a test using startsWith

    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Address")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("prop" -> i.toString), "Address")
      }
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop LIKE 'ww_' RETURN a")

    result should (use("Filter", IndexSeekByRange.name) and evaluateTo(List(Map("a" -> a2))))
  }

  ignore("should plan an IndexRangeSeek for a string search that starts with a prefix when index exists") {// Replace with a test using startsWith

    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Address")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("prop" -> i.toString), "Address")
      }
    }
    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createIndex("Address", "prop")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop LIKE 'ww%w%' RETURN a")

    result should (use(IndexSeekByRange.name, "Filter") and evaluateTo(List(Map("a" -> a1), Map("a" -> a2))))
  }

  ignore("should plan a UniqueIndexSeek when constraint exists") {// Replace with a test using startsWith

    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Address")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("prop" -> i.toString), "Address")
      }
    }

    val a1 = createLabeledNode(Map("prop" -> "www123"), "Address")
    val a2 = createLabeledNode(Map("prop" -> "www"), "Address")
    val a3 = createLabeledNode(Map("prop" -> "ww"), "Address")

    graph.createConstraint("Address", "prop")

    val result = executeWithAllPlanners("MATCH (a:Address) WHERE a.prop LIKE 'www%' RETURN a")

    result should (use(UniqueIndexSeekByRange.name) and evaluateTo(List(Map("a" -> a1), Map("a" -> a2))))
  }

  test("should be able to plan index seek for numerical less than") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0))))
  }

  test("should be able to plan index seek for numerical negated greater than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop >= 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0))))
  }

  test("should be able to plan index seek for numerical less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 10),
        Map("prop" -> 10.0)
      )))
  }

  test("should be able to plan index seek for numerical negated greater than") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 10),
        Map("prop" -> 10.0)
      )))
  }

  test("should be able to plan index seek for numerical greater than") {
    // Given matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > 5 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlannersReplaceNaNs(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for numerical negated less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop <= 5 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlannersReplaceNaNs(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for numerical greater than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= 5 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlannersReplaceNaNs(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for numerical negated less than") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE NOT n.prop < 5 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlannersReplaceNaNs(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for textual less than") {
    // Given matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> s"15${java.lang.Character.MIN_VALUE}"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("", "-5", "0", "10", "14whatever"))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for textual less than or equal") {
    // Given matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> s"15${java.lang.Character.MIN_VALUE}"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.columnAs[String]("prop").toSet should equal(Set("", "-5", "0", "10", "15", "14whatever"))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for textual greater than") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.columnAs[String]("prop").toList should equal(Seq(smallValue, "5" ,"5"))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek for textual greater than or equal") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("15", smallValue, "5", "5"))
    result should use("NodeIndexSeekByRange")
  }

  test("should be able to plan index seek without confusing property key ids") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given Non-matches
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop2" -> 5), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop2" -> 10), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '15' AND n.prop2 > 5 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then

    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to plan index seek for empty numerical between range") {
    // Given
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= 10 AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to plan index seek for numerical null range") {
    // Given
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop <= null RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to plan index seek for non-empty numerical between range") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 6.1), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >=5 AND n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 6.1)
      )))
  }

  test("should be able to plan index seek using multiple non-overlapping numerical ranges") {
    // Given matches
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")
    createLabeledNode(Map("prop" -> 6.1), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 12.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= 0 AND n.prop >=5 AND n.prop < 10 AND n.prop < 100 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 6.1)
      )))
  }

  test("should be able to plan index seek using empty textual range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < '15' AND n.prop >= '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to plan index seek using textual null range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop < null RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to plan index seek using non-empty textual range") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '10' AND n.prop < '15' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> "10"),
        Map("prop" -> "14whatever"
        ))))
  }

  test("should be able to plan index seek using multiple non-overlapping textual ranges") {
    val smallValue = s"15${java.lang.Character.MIN_VALUE}"

    // Given matches
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> smallValue), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop >= '10' AND n.prop < '15' AND n.prop <= '14whatever' RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and
      evaluateTo(List(
        Map("prop" -> "10"),
        Map("prop" -> "14whatever"
        ))))
  }

  test("should be able to execute index seek using inequalities over different types as long as one inequality yields no results (1)") {
    // Given
    createLabeledNode(Map("prop" -> "15"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should be able to execute index seek using inequalities over different types as long as one inequality yields no results (2)") {
    // Given
    createLabeledNode(Map("prop" -> 15), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop > '1' AND n.prop > 10 RETURN n.prop AS prop"

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
  }

  test("should refuse to execute index seeks using inequalities over different types") {
    // Given
    createLabeledNode(Map("prop" -> 15), "Label")
    createLabeledNode(Map("prop" -> "1"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '1' AND n.prop > 10 RETURN n.prop AS prop"

    an[IllegalArgumentException] should be thrownBy {
      executeWithAllPlanners(query).toList
    }

    executeWithAllPlanners(s"EXPLAIN $query") should use("NodeIndexSeekByRange")
  }

  test("should refuse to execute index seeks using inequalities over incomparable types (detected at compile time)") {
    // Given
    val query = "MATCH (n:Label) WHERE n.prop >= [1, 2, 3] RETURN n.prop AS prop"

    an[SyntaxException] should be thrownBy {
      executeWithAllPlanners(query).toList
    }
  }

  test("should refuse to execute index seeks using inequalities over incomparable types (detected at runtime)") {
    // Given
    (1 to 405).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} RETURN n.prop AS prop"

    an[IllegalArgumentException] should be thrownBy {
      executeWithAllPlanners(query, "param" -> Array[Int](1, 2, 3)).toList
    }

    executeWithAllPlanners(s"EXPLAIN $query") should use("NodeIndexSeekByRange")
  }

  test("should return no rows when executing index seeks using inequalities over incomparable types but also comparing against null") {
    // Given
    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} AND n.prop < null RETURN n.prop AS prop"

    executeWithAllPlanners(query, "param" -> Array[Int](1, 2, 3)) should (use("NodeIndexSeekByRange") and be(empty))
  }

  test("should plan range index seeks matching characters against properties (coerced to string wrt the inequality)") {
    // Given
    val nonMatchingChar = "X".charAt(0).charValue()
    val matchingChar = "Y".charAt(0).charValue()

    (1 to 500).foreach { _ =>
      createLabeledNode("Label")
    }
    createLabeledNode(Map("prop" -> matchingChar), "Label")
    createLabeledNode(Map("prop" -> matchingChar.toString), "Label")
    createLabeledNode(Map("prop" -> nonMatchingChar), "Label")
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} RETURN n.prop AS prop"

    val result = executeWithAllPlanners(query, "param" -> matchingChar)

    result should (use("NodeIndexSeekByRange") and evaluateTo(List(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString))))
  }

  test("should plan range index seeks matching strings against character properties (coerced to string wrt the inequality)") {
    // Given
    val nonMatchingChar = "X".charAt(0).charValue()
    val matchingChar = "Y".charAt(0).charValue()

    (1 to 500).foreach { _ =>
      createLabeledNode("Label")
    }
    createLabeledNode(Map("prop" -> matchingChar), "Label")
    createLabeledNode(Map("prop" -> matchingChar.toString), "Label")
    createLabeledNode(Map("prop" -> nonMatchingChar), "Label")
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} RETURN n.prop AS prop"

    val result = executeWithAllPlanners(query, "param" -> matchingChar.toString)

    result should (use("NodeIndexSeekByRange") and evaluateTo(List(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString))))
  }

  test("rule planner should plan index seek for inequality match") {
    graph.createIndex("Label", "prop")
    createLabeledNode(Map("prop" -> 1), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 CREATE () RETURN n.prop"

    val result = executeWithRulePlanner(query)

    result should (use("SchemaIndex") and evaluateTo(List(Map("n.prop" -> 1), Map("n.prop" -> 5))))
  }

}
