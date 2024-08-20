/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.graphcounts

import org.json4s.Formats
import org.json4s.StringInput
import org.json4s.native.Json
import org.json4s.native.JsonMethods
import org.neo4j.cypher.graphcounts.GraphCountsJson.allFormats
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.scalatest.prop.TableDrivenPropertyChecks

class GraphCountsJsonTest extends CypherFunSuite {

  implicit val formats: Formats = allFormats

  test("Constraint") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "label": "DeprecatedRelyingParty",
        |    "properties": [
        |      "relyingPartyId"
        |    ],
        |    "type": "Uniqueness constraint"
        |}
      """.stripMargin
    )).extract[Constraint] should be(
      Constraint(
        Some("DeprecatedRelyingParty"),
        None,
        List("relyingPartyId"),
        ConstraintType.UNIQUE,
        Nil
      )
    )
  }

  test("Relationship Existence Constraint") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "relationshipType": "Foo",
        |    "properties": [
        |      "relyingPartyId"
        |    ],
        |    "type": "Existence constraint"
        |}
      """.stripMargin
    )).extract[Constraint] should be(
      Constraint(
        None,
        Some("Foo"),
        List("relyingPartyId"),
        ConstraintType.EXISTS,
        Nil
      )
    )
  }

  test("index for label") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 2,
        |    "labels": [
        |        "Person"
        |    ],
        |    "properties": [
        |        "uuid"
        |    ],
        |    "totalSize": 2,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "RANGE",
        |    "indexProvider": "range-1.0"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(
        Some(Seq("Person")),
        None,
        IndexType.RANGE,
        Seq("uuid"),
        2,
        2,
        0,
        new IndexProviderDescriptor("range", "1.0")
      )
    )
  }

  test("index for relationship type") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 2,
        |    "relationshipTypes": [
        |        "REL"
        |    ],
        |    "properties": [
        |        "prop"
        |    ],
        |    "totalSize": 2,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "RANGE",
        |    "indexProvider": "range-1.0"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(None, Some(Seq("REL")), IndexType.RANGE, Seq("prop"), 2, 2, 0, new IndexProviderDescriptor("range", "1.0"))
    )
  }

  test("lookup index for label") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 0,
        |    "labels": [],
        |    "totalSize": 0,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "LOOKUP",
        |    "indexProvider": "token-lookup-1.0"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(Some(Seq()), None, IndexType.LOOKUP, Seq(), 0, 0, 0, new IndexProviderDescriptor("token-lookup", "1.0"))
    )
  }

  test("lookup index for relationship type") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 0,
        |    "relationshipTypes": [],
        |    "totalSize": 0,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "LOOKUP",
        |    "indexProvider": "token-lookup-1.0"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(None, Some(Seq()), IndexType.LOOKUP, Seq(), 0, 0, 0, new IndexProviderDescriptor("token-lookup", "1.0"))
    )
  }

  test("NodeLCount with Label") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 2,
        |    "label": "Person"
        |}
      """.stripMargin
    )).extract[NodeCount] should be(
      NodeCount(2, Some("Person"))
    )
  }

  test("NodeCount") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 2,
        |}
      """.stripMargin
    )).extract[NodeCount] should be(
      NodeCount(2, None)
    )
  }

  test("RelationshipCount") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |}
      """.stripMargin
    )).extract[RelationshipCount] should be(
      RelationshipCount(5, None, None, None)
    )
  }

  test("RelationshipCount with type") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |}
      """.stripMargin
    )).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), None, None)
    )
  }

  test("RelationshipCount with type and startLabel") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |    "startLabel": "RelyingParty"
        |}
      """.stripMargin
    )).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), Some("RelyingParty"), None)
    )
  }

  test("RelationshipCount with type and endLabel") {
    JsonMethods.parse(StringInput(
      """
        |{
        |    "count": 5,
        |    "relationshipType": "HAS_SSL_CERTIFICATE",
        |    "endLabel": "RelyingParty"
        |}
      """.stripMargin
    )).extract[RelationshipCount] should be(
      RelationshipCount(5, Some("HAS_SSL_CERTIFICATE"), None, Some("RelyingParty"))
    )
  }

  test("GraphCountData") {
    JsonMethods.parse(StringInput(
      """
        |{"constraints": [{
        |    "label": "SSLCertificate",
        |    "properties": [
        |        "serialNumber"
        |    ],
        |    "type": "Uniqueness constraint"
        |}, {
        |    "label": "SSLCertificate",
        |    "properties": [
        |      "serialNumber"
        |    ],
        |    "type": "Property type constraint",
        |    "propertyTypes": [
        |      "STRING"
        |    ]
        |}],
        |"indexes": [{
        |    "estimatedUniqueSize": 4,
        |    "labels": [
        |        "SSLCertificate"
        |    ],
        |    "properties": [
        |        "serialNumber"
        |    ],
        |    "totalSize": 4,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "RANGE",
        |    "indexProvider" : "range-1.0"
        |}],
        |"nodes": [{
        |    "count": 1,
        |    "label": "VettingProvider"
        |}],
        |"relationships": [{
        |    "count": 1,
        |    "relationshipType": "HAS_GEOLOCATION",
        |    "startLabel": "Address"
        |}]}
      """.stripMargin
    )).extract[GraphCountData] should be(
      GraphCountData(
        Seq(
          Constraint(
            label = Some("SSLCertificate"),
            relationshipType = None,
            properties = List("serialNumber"),
            `type` = ConstraintType.UNIQUE,
            propertyTypes = Nil
          ),
          Constraint(
            label = Some("SSLCertificate"),
            relationshipType = None,
            properties = List("serialNumber"),
            `type` = ConstraintType.PROPERTY_TYPE,
            propertyTypes = List(SchemaValueType.STRING)
          )
        ),
        Seq(Index(
          Some(Seq("SSLCertificate")),
          None,
          IndexType.RANGE,
          Seq("serialNumber"),
          4,
          4,
          0,
          new IndexProviderDescriptor("range", "1.0")
        )),
        Seq(NodeCount(1, Some("VettingProvider"))),
        Seq(RelationshipCount(1, Some("HAS_GEOLOCATION"), Some("Address"), None))
      )
    )
  }
}

class ConstraintsJsonTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private val constraints: List[(String, Constraint)] =
    List(
      """{
        |    "label": "Label",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Uniqueness constraint"
        |}""".stripMargin ->
        Constraint(
          label = Some("Label"),
          relationshipType = None,
          properties = List("prop"),
          `type` = ConstraintType.UNIQUE,
          propertyTypes = Nil
        ),
      """{
        |    "relationshipType": "REL",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Uniqueness constraint"
        |}""".stripMargin ->
        Constraint(
          label = None,
          relationshipType = Some("REL"),
          properties = List("prop"),
          `type` = ConstraintType.UNIQUE,
          propertyTypes = Nil
        ),
      """{
        |    "label": "Label",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Existence constraint"
        |}""".stripMargin ->
        Constraint(
          label = Some("Label"),
          relationshipType = None,
          properties = List("prop"),
          `type` = ConstraintType.EXISTS,
          propertyTypes = Nil
        ),
      """{
        |    "relationshipType": "REL",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Existence constraint"
        |}""".stripMargin ->
        Constraint(
          label = None,
          relationshipType = Some("REL"),
          properties = List("prop"),
          `type` = ConstraintType.EXISTS,
          propertyTypes = Nil
        ),
      """{
        |    "label": "Label",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Node Key"
        |}""".stripMargin ->
        Constraint(
          label = Some("Label"),
          relationshipType = None,
          properties = List("prop"),
          `type` = ConstraintType.UNIQUE_EXISTS,
          propertyTypes = Nil
        ),
      """{
        |    "relationshipType": "REL",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Node Key"
        |}""".stripMargin ->
        Constraint(
          label = None,
          relationshipType = Some("REL"),
          properties = List("prop"),
          `type` = ConstraintType.UNIQUE_EXISTS,
          propertyTypes = Nil
        ),
      """{
        |    "label": "Label",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Property type constraint",
        |    "propertyTypes": [
        |      "INTEGER"
        |    ]
        |}""".stripMargin ->
        Constraint(
          label = Some("Label"),
          relationshipType = None,
          properties = List("prop"),
          `type` = ConstraintType.PROPERTY_TYPE,
          propertyTypes = List(SchemaValueType.INTEGER)
        ),
      """{
        |    "relationshipType": "REL",
        |    "properties": [
        |      "prop"
        |    ],
        |    "type": "Property type constraint",
        |    "propertyTypes": [
        |      "INTEGER"
        |    ]
        |}""".stripMargin ->
        Constraint(
          label = None,
          relationshipType = Some("REL"),
          properties = List("prop"),
          `type` = ConstraintType.PROPERTY_TYPE,
          propertyTypes = List(SchemaValueType.INTEGER)
        ),
      """{
        |    "relationshipType": "REL",
        |    "type": "Relationship endpoint label constraint",
        |    "endpointLabelId": "Integer"
        |}""".stripMargin ->
        Constraint(
          label = None,
          relationshipType = Some("REL"),
          properties = List.empty,
          `type` = ConstraintType.ENDPOINT,
          propertyTypes = Nil
        ),
      """{
        |    "label": "Label",
        |    "type": "Label coexistence constraint",
        |    "requiredLabelId": "Integer"
        |}""".stripMargin ->
        Constraint(
          label = Some("Label"),
          relationshipType = None,
          properties = List.empty,
          `type` = ConstraintType.LABEL_COEXISTENCE,
          propertyTypes = Nil
        )
    )

  private val constraintsTable =
    Table(("Constraint", "JSON"), constraints: _*)

  /*
  Ensures that all types of constraints are covered when parsing GraphCounts, while remaining loosely coupled.
  If this test fails, please ensure that the new constraint gets deserialized properly, and add a test case to the list of constraints above.
   */
  test("should cover all constraint types") {
    val constraintTypes = constraintsTable.view.map(_._2.`type`).toSet
    ConstraintType.values().toSet.diff(constraintTypes) shouldBe empty
  }

  test("should deserialize all constraints") {
    forAll(constraintsTable) { (json: String, constraint: Constraint) =>
      Json.apply(allFormats).read[Constraint](json) shouldEqual constraint
    }
  }

}
