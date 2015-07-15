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

import org.neo4j.cypher.{SyntaxException, ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport}

class WhereAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("NOT(p1 AND p2) should return true when p2 is false") {
    createNode("apa")

    val result = executeWithAllPlanners("match n where not(n.name = 'apa' and false) return n")

    result should have size 1
  }

  test("should throw exception if comparing string and number") {
    createLabeledNode(Map("prop" -> "15"), "Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    a[IncomparableValuesException] should be thrownBy executeWithCostPlannerOnly(query)
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").asMultiSet should equal(MultiSet(Double.NegativeInfinity, -5, 0, 5, 5.0))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").asMultiSet should equal(MultiSet(Double.NegativeInfinity, -5, 0, 5, 5.0, 10, 10.0))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues.asMultiSet should equal(MultiSet(10, 10.0, 100, Double.PositiveInfinity))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues.asMultiSet should equal(MultiSet(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet("", "-5", "0", "10", "14whatever"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet("", "-5", "0", "10", "15", "14whatever"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet(smallValue, "5" ,"5"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet("15", smallValue, "5", "5"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").asMultiSet should equal(MultiSet(5, 5.0, 6.1))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").asMultiSet should equal(MultiSet(5, 5.0, 6.1))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet("10", "14whatever"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").asMultiSet should equal(MultiSet("10", "14whatever"))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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
    val result = executeWithCostPlannerOnly(query)

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
      executeWithCostPlannerOnly(query).toList
    }

    executeWithCostPlannerOnly(s"EXPLAIN $query").executionPlanDescription().toString should include("NodeIndexSeekByRange")
  }

  test("should refuse to execute index seeks using inequalities over incomparable types (detected at compile time)") {
    // Given
    val query = "MATCH (n:Label) WHERE n.prop >= [1, 2, 3] RETURN n.prop AS prop"

    an[SyntaxException] should be thrownBy {
      executeWithCostPlannerOnly(query).toList
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
      executeWithCostPlannerOnly(query, "param" -> Array[Int](1, 2, 3)).toList
    }

    executeWithCostPlannerOnly(s"EXPLAIN $query").executionPlanDescription().toString should include("NodeIndexSeekByRange")
  }

  test("should return no rows when executing index seeks using inequalities over incomparable types but also comparing against null") {
    // Given
    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} AND n.prop < null RETURN n.prop AS prop"

    executeWithCostPlannerOnly(query, "param" -> Array[Int](1, 2, 3)).toList should equal(List.empty)
    executeWithCostPlannerOnly(s"EXPLAIN $query").executionPlanDescription().toString should include("NodeIndexSeekByRange")
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

    val result = executeWithCostPlannerOnly(query, "param" -> matchingChar)

    result.columnAs[Any]("prop").asMultiSet should equal(MultiSet(matchingChar, matchingChar.toString))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
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

    val result = executeWithCostPlannerOnly(query, "param" -> matchingChar.toString)

    result.columnAs[Any]("prop").asMultiSet should equal(MultiSet(matchingChar, matchingChar.toString))
    result.executionPlanDescription().toString should include("NodeIndexSeekByRange")
  }

  object MultiSet {
    def apply[T](values: T*) = values.iterator.asMultiSet
  }

  implicit class SeqMultiMapConverter[T](data: Seq[T]) {
    def asMultiSet: Map[T, Int] = data.iterator.asMultiSet
  }

  implicit class IteratorMultiMapConverter[T](data: Iterator[T]) {
    def asMultiSet: Map[T, Int] = {
      data.foldLeft(Map.empty[T, Int]) {
        case (acc, elt) => acc + (elt -> acc.get(elt).map(_ + 1).getOrElse(1))
      }
    }
  }
}
