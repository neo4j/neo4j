/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexSeekByRange, UniqueIndexSeekByRange}
import org.neo4j.cypher.{ExecutionEngineFunSuite, SyntaxException}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

/**
  * These tests are testing the actual index implementation, thus they should all check the actual result.
  * If you only want to verify that plans using indexes are actually planned, please use
  * [[org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LeafPlanningIntegrationTest]]
  */
class NodeIndexSeekByRangeAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle comparing large integers") {
    // Given
    val person = createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWith(Configs.Interpreted, "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486378 RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("p" -> person)))
  }

  test("should handle comparing large integers 2") {
    // Given
    createLabeledNode(Map("age" -> 5987523281782486379L), "Person")


    graph.createIndex("Person", "age")

    // When
    val result = executeWith(Configs.Interpreted, "MATCH (p:Person) USING INDEX p:Person(age) WHERE p.age > 5987523281782486379 RETURN p",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
  }

  test("should be case sensitive for STARTS WITH with indexes") {
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

    val query = "MATCH (l:Location) WHERE l.name STARTS WITH 'Lon' RETURN l"

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should perform prefix search in an update query") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "london"), "Location")
    for (i <- 1 to 100) createLabeledNode(Map("name" -> ("City" + i)), "Location")
    graph.createIndex("Location", "name")

    val query =
      """MATCH (l:Location) WHERE l.name STARTS WITH 'Lon'
        |CREATE (L:Location {name: toUpper(l.name)})
        |RETURN L.name AS NAME""".stripMargin

    val result = executeWith(Configs.UpdateConf, query)

    result.toList should equal(List(Map("NAME" -> "LONDON")))
  }

  test("should only match on the actual prefix") {
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

    val query = "MATCH (l:Location) WHERE l.name STARTS WITH 'Lon' RETURN l"

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    result.toList should equal(List(Map("l" -> london)))
  }

  test("should plan the leaf with the longest prefix if multiple STARTS WITH patterns") {

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

    // Add an uninteresting predicate using a parameter to stop autoparameterization from happening
    val result = executeWith(Configs.Interpreted, """MATCH (a:Address)
            |WHERE 43 = {apa}
            |  AND a.prop STARTS WITH 'w'
            |  AND a.prop STARTS WITH 'www'
            |RETURN a""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners), params = Map("apa" -> 43))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
    result.executionPlanDescription().toString should include("prop STARTS WITH \"www\"")
  }

  test("should plan an IndexRangeSeek for a STARTS WITH predicate search when index exists") {
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

    val result = executeWith(Configs.Interpreted, "MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
  }

  test("should plan a UniqueIndexSeek when constraint exists") {

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

    val result = executeWith(Configs.Interpreted, "MATCH (a:Address) WHERE a.prop STARTS WITH 'www' RETURN a",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(UniqueIndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    result.toSet should equal(Set(Map("a" -> a1), Map("a" -> a2)))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0)))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should
      equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0)
      ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should
      equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 10),
        Map("prop" -> 10.0)
      ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
        Map("prop" -> Double.NegativeInfinity),
        Map("prop" -> -5),
        Map("prop" -> 0),
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 10),
        Map("prop" -> 10.0)
    ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    // values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    //values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(10, 10.0, 100, Double.PositiveInfinity))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    //values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    val values = result.columnAs[Number]("prop").toSeq
    // TODO: this check should not be here, waiting for cypher to update NaN treatment behaviour
    // values.exists(d => java.lang.Double.isNaN(d.doubleValue())) should be(right = true)
    val saneValues = values.filter(d => !java.lang.Double.isNaN(d.doubleValue()))
    saneValues should equal(Seq(5, 5.0, 10, 10.0, 100, Double.PositiveInfinity))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("", "-5", "0", "10", "14whatever"))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.columnAs[String]("prop").toSet should equal(Set("", "-5", "0", "10", "15", "14whatever"))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq(smallValue, "5", "5"))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.columnAs[String]("prop").toList should equal(Seq("15", smallValue, "5", "5"))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then

    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 6.1)
    ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
        Map("prop" -> 5),
        Map("prop" -> 5.0),
        Map("prop" -> 6.1)
    ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
      Map("prop" -> "10"),
      Map("prop" -> "14whatever")
    ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(
      Map("prop" -> "10"),
      Map("prop" -> "14whatever")
    ))
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
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    result should be(empty)
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
    val result = executeWith(Configs.Interpreted, query)

    // Then
    result.columnAs[String]("prop").toList should equal(List.empty)
    result.executionPlanDescription().toString should include(IndexSeekByRange.name)
  }

  // TODO: re-enable linting for these queries where some predicates can be statically determined to always be false.
  ignore("should refuse to execute index seeks using inequalities over different types") {
    // Given
    createLabeledNode(Map("prop" -> 15), "Label")
    createLabeledNode(Map("prop" -> "1"), "Label")

    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= '1' AND n.prop > 10 RETURN n.prop AS prop"

    executeWith(Configs.Interpreted, s"EXPLAIN $query",
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    an[IllegalArgumentException] should be thrownBy {
      executeWith(Configs.Empty, query).toList
    }
  }

  test("should refuse to execute index seeks using inequalities over incomparable types (detected at compile time)") {
    // Given
    val query = "MATCH (n:Label) WHERE n.prop >= [1, 2, 3] RETURN n.prop AS prop"
    a [SyntaxException] should be thrownBy {
      executeWith(Configs.Empty, query).toList
    }
  }

  test("should yield empty results for index seeks using inequalities over incomparable types detected at runtime") {
    // Given
    (1 to 405).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} RETURN n.prop AS prop"

    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }), params = Map("param" -> Array[Int](1, 2, 3)))
    result.toList should be(empty)
  }

  test("should return no rows when executing index seeks using inequalities over incomparable types but also comparing against null") {
    // Given
    (1 to 400).foreach { _ =>
      createLabeledNode("Label")
    }
    graph.createIndex("Label", "prop")

    val query = "MATCH (n:Label) WHERE n.prop >= {param} AND n.prop < null RETURN n.prop AS prop"

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners), params = Map("param" -> Array[Int](1, 2, 3)))

    result should be(empty)
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

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners), params = Map("param" -> matchingChar))

    result.toSet should equal(Set(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString)))
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

    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners), params = Map("param" -> matchingChar.toString))

    result.toSet should equal(Set(Map("prop" -> matchingChar), Map("prop" -> matchingChar.toString)))
  }

  test("rule planner should plan index seek for inequality match") {
    graph.createIndex("Label", "prop")
    createLabeledNode(Map("prop" -> 1), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 10), "Label")
    for (i <- 1 to 300) createLabeledNode("Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 CREATE () RETURN n.prop"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    result.toList should equal(List(Map("n.prop" -> 1), Map("n.prop" -> 5)))
  }

  test("should not use index seek by range when rhs of > inequality depends on property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop > a.prop RETURN count(a) as c"
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan shouldNot useOperators(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size / 2)))
  }

  test("should not use index seek by range when rhs of <= inequality depends on property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop <= a.prop RETURN count(a) as c"
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan shouldNot useOperators(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size / 2)))
  }

  test("should not use index seek by range when rhs of >= inequality depends on same property") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = "MATCH (a)-->(b:Label) WHERE b.prop >= b.prop RETURN count(a) as c"
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan shouldNot useOperators(IndexSeekByRange.name)
      }))

    // Then
    result.toList should equal(List(Map("c" -> size)))
  }

  test("should use index seek by range with literal on the lhs of inequality") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = s"MATCH (a)-->(b:Label) WHERE ${size / 2} < b.prop RETURN count(a) as c"
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    assert(size > 20)
    result.toList should equal(List(Map("c" -> (size / 2))))
  }

  test("should use index seek by range with double inequalities") {
    // Given
    val size = createTestModelBigEnoughToConsiderPickingIndexSeek

    // When
    val query = s"MATCH (a)-->(b:Label) WHERE 10 < b.prop <= ${size - 10} RETURN count(a) as c"
    val result = executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators(IndexSeekByRange.name)
      }, Configs.AllRulePlanners))

    // Then
    assert(size > 20)
    result.toList should equal(List(Map("c" -> (size - 20))))
  }

  test("should use the index of inequality range scans") {
    graph.inTx {
      (1 to 60).foreach { i =>
        createLabeledNode(Map("gender" -> "male"), "Person")
      }
      (1 to 30).foreach { i =>
        createLabeledNode(Map("gender" -> "female"), "Person")
      }
      (1 to 2).foreach { i =>
        createLabeledNode("Person")
      }
    }

    graph.createIndex("Person", "gender")

    val result = graph.execute("CYPHER PROFILE MATCH (a:Person) WHERE a.gender > 'female' RETURN count(a) as c")

    import scala.collection.JavaConverters._
    result.asScala.toList.map(_.asScala) should equal(List(Map("c" -> 60)))
    result.getExecutionPlanDescription.toString should include(IndexSeekByRange.name)
    result.close()
  }

  private def createTestModelBigEnoughToConsiderPickingIndexSeek: Int = {
    val size = 400

    graph.createIndex("Label", "prop")

    (1 to size).foreach { i =>
      // Half of unlabeled nodes has prop = 0 (for even i:s prop = 0, for odd i:s prop = i)
      val a = createNode(Map("prop" -> (i & 1) * i))
      val b = createLabeledNode(Map("prop" -> i), "Label")
      relate(a, b)
    }
    size
  }
}
