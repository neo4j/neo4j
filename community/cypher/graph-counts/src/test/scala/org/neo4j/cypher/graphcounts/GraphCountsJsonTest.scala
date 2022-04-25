/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.graphcounts

import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.StringInput
import org.json4s.native.JsonMethods
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexType

class GraphCountsJsonTest extends CypherFunSuite {

  implicit val formats: Formats = DefaultFormats + RowSerializer

  test("Constraint") {
    implicit val formats: Formats = DefaultFormats + ConstraintTypeSerializer
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
        ConstraintType.UNIQUE
      )
    )
  }

  test("Relationship Existence Constraint") {
    implicit val formats: Formats = DefaultFormats + ConstraintTypeSerializer
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
        ConstraintType.EXISTS
      )
    )
  }

  test("index for label") {
    implicit val formats: Formats = DefaultFormats + IndexTypeSerializer
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
        |    "indexType": "RANGE"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(Some(Seq("Person")), None, IndexType.RANGE, Seq("uuid"), 2, 2, 0)
    )
  }

  test("index for relationship type") {
    implicit val formats: Formats = DefaultFormats + IndexTypeSerializer
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
        |    "indexType": "RANGE"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(None, Some(Seq("REL")), IndexType.RANGE, Seq("prop"), 2, 2, 0)
    )
  }

  test("lookup index for label") {
    implicit val formats: Formats = DefaultFormats + IndexTypeSerializer
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 0,
        |    "labels": [],
        |    "totalSize": 0,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "LOOKUP"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(Some(Seq()), None, IndexType.LOOKUP, Seq(), 0, 0, 0)
    )
  }

  test("lookup index for relationship type") {
    implicit val formats: Formats = DefaultFormats + IndexTypeSerializer
    JsonMethods.parse(StringInput(
      """
        |{
        |    "estimatedUniqueSize": 0,
        |    "relationshipTypes": [],
        |    "totalSize": 0,
        |    "updatesSinceEstimation": 0,
        |    "indexType": "LOOKUP"
        |}
      """.stripMargin
    )).extract[Index] should be(
      Index(None, Some(Seq()), IndexType.LOOKUP, Seq(), 0, 0, 0)
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
    implicit val formats: Formats = DefaultFormats + IndexTypeSerializer + ConstraintTypeSerializer
    JsonMethods.parse(StringInput(
      """
        |{"constraints": [{
        |    "label": "SSLCertificate",
        |    "properties": [
        |        "serialNumber"
        |    ],
        |    "type": "Uniqueness constraint"
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
        |    "indexType": "RANGE"
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
        Seq(Constraint(Some("SSLCertificate"), None, Seq("serialNumber"), ConstraintType.UNIQUE)),
        Seq(Index(Some(Seq("SSLCertificate")), None, IndexType.RANGE, Seq("serialNumber"), 4, 4, 0)),
        Seq(NodeCount(1, Some("VettingProvider"))),
        Seq(RelationshipCount(1, Some("HAS_GEOLOCATION"), Some("Address"), None))
      )
    )
  }
}
