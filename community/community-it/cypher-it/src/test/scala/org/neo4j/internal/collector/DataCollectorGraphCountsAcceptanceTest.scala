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
package org.neo4j.internal.collector

import org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.GraphIcing
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.token.TokenHolders

import scala.jdk.CollectionConverters.IteratorHasAsScala

class DataCollectorGraphCountsAcceptanceTest extends ExecutionEngineFunSuite with GraphIcing with SampleGraphs {

  // Make sure that background sampling is disabled so we can test `updatesSinceEstimation`
  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() + (index_background_sampling_enabled -> java.lang.Boolean.FALSE)

  test("retrieve empty") {
    // setup
    graph.withTx(tx => {
      tx.schema().getIndexes.iterator().asScala.foreach(index => index.drop())
      tx.commit()
    })

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    res("section") should be("GRAPH COUNTS")
    seq(res("data"), "nodes") should contain only Map("count" -> 0)
    seq(res("data"), "relationships") should contain only Map("count" -> 0)
    seq(res("data"), "indexes") should be(Seq())
    seq(res("data"), "constraints") should be(Seq())
  }

  test("retrieve all index types") {
    // all but FULLTEXT, as this is currently not exported.
    graph.withTx(tx => {
      tx.schema().getIndexes.iterator().asScala.foreach(index => index.drop())
      tx.commit()
    })

    graph.createLookupIndex(isNodeIndex = true)
    graph.createNodeIndex(IndexType.TEXT, "Label", "textProp")
    graph.createNodeIndex(IndexType.POINT, "Label", "pointProp")
    graph.createNodeIndex(IndexType.RANGE, "Label", "rangeProp")

    graph.createLookupIndex(isNodeIndex = false)
    graph.createRelationshipIndex(IndexType.TEXT, "RelationshipType", "textProp")
    graph.createRelationshipIndex(IndexType.POINT, "RelationshipType", "pointProp")
    graph.createRelationshipIndex(IndexType.RANGE, "RelationshipType", "rangeProp")

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    seq(res("data"), "indexes") should contain.only(
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.LOOKUP.name,
        "properties" -> Seq.empty,
        "labels" -> Seq.empty,
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "token-lookup-1.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.TEXT.name,
        "properties" -> Seq("textProp"),
        "labels" -> Seq("Label"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "text-2.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.POINT.name,
        "properties" -> Seq("pointProp"),
        "labels" -> Seq("Label"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "point-1.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.RANGE.name,
        "properties" -> Seq("rangeProp"),
        "labels" -> Seq("Label"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "range-1.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.LOOKUP.name,
        "properties" -> Seq.empty,
        "relationshipTypes" -> Seq.empty,
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "token-lookup-1.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.TEXT.name,
        "properties" -> Seq("textProp"),
        "relationshipTypes" -> Seq("RelationshipType"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "text-2.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.POINT.name,
        "properties" -> Seq("pointProp"),
        "relationshipTypes" -> Seq("RelationshipType"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "point-1.0"
      ),
      Map(
        "totalSize" -> 0,
        "indexType" -> IndexType.RANGE.name,
        "properties" -> Seq("rangeProp"),
        "relationshipTypes" -> Seq("RelationshipType"),
        "updatesSinceEstimation" -> 0,
        "estimatedUniqueSize" -> 0,
        "indexProvider" -> "range-1.0"
      )
    )
  }

  test("retrieve nodes") {
    // given
    createNode()
    createLabeledNode("User")
    createLabeledNode("User")
    createLabeledNode("Donkey")

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    seq(res("data"), "nodes") should contain.only(
      Map("count" -> 4),
      Map("count" -> 2, "label" -> "User"),
      Map("count" -> 1, "label" -> "Donkey")
    )
  }

  test("retrieve relationships") {
    // given
    val n1 = createNode()
    val n2 = createLabeledNode("User")
    relate(n1, n1, "R")
    relate(n1, n2, "R")
    relate(n2, n1, "R2")
    relate(n2, n2, "R")

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    seq(res("data"), "relationships") should contain.only(
      Map("count" -> 4),
      Map("count" -> 3, "relationshipType" -> "R"),
      Map("count" -> 1, "relationshipType" -> "R", "startLabel" -> "User"),
      Map("count" -> 2, "relationshipType" -> "R", "endLabel" -> "User"),
      Map("count" -> 1, "relationshipType" -> "R2"),
      Map("count" -> 1, "relationshipType" -> "R2", "startLabel" -> "User")
    )
  }

  test("retrieve complex graph") {
    // given
    createSteelfaceGraph()

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    assertSteelfaceGraphCounts(
      res,
      TokenNames("User", "Car", "Room", "OWNS", "STAYS_IN", "email", "lastName", "firstName", "number", "hotel")
    )
  }

  test("retrieve anonymized complex graph") {
    // given
    createSteelfaceGraph()

    // when
    val res = execute("CALL db.stats.retrieveAllAnonymized('myGraphToken')").toList.filter(row =>
      row("section") == "GRAPH COUNTS"
    ).head

    // then
    val tokens = graph.getDependencyResolver.resolveDependency(classOf[TokenHolders]).propertyKeyTokens
    assertSteelfaceGraphCounts(
      res,
      TokenNames(
        "L0",
        "L1",
        "L2",
        "R0",
        "R1",
        "p" + tokens.getIdByName("email"),
        "p" + tokens.getIdByName("lastName"),
        "p" + tokens.getIdByName("firstName"),
        "p" + tokens.getIdByName("number"),
        "p" + tokens.getIdByName("hotel")
      )
    )
  }

  case class TokenNames(
    User: String,
    Car: String,
    Room: String,
    OWNS: String,
    STAYS_IN: String,
    email: String,
    lastName: String,
    firstName: String,
    number: String,
    hotel: String
  )

  private def assertSteelfaceGraphCounts(res: Map[String, AnyRef], tokenNames: TokenNames): Unit = {

    res("section") should be("GRAPH COUNTS")
    seq(res("data"), "nodes") should contain.only(
      Map("count" -> 1278),
      Map("label" -> tokenNames.User, "count" -> 1000),
      Map("label" -> tokenNames.Car, "count" -> 128),
      Map("label" -> tokenNames.Room, "count" -> 150)
    )
    seq(res("data"), "relationships") should contain.only(
      Map("count" -> 320),
      Map("relationshipType" -> tokenNames.OWNS, "count" -> 170),
      Map("relationshipType" -> tokenNames.OWNS, "startLabel" -> tokenNames.User, "count" -> 170),
      Map("relationshipType" -> tokenNames.OWNS, "endLabel" -> tokenNames.Car, "count" -> 100),
      Map("relationshipType" -> tokenNames.OWNS, "endLabel" -> tokenNames.Room, "count" -> 70),
      Map("relationshipType" -> tokenNames.STAYS_IN, "count" -> 150),
      Map("relationshipType" -> tokenNames.STAYS_IN, "startLabel" -> tokenNames.User, "count" -> 150),
      Map("relationshipType" -> tokenNames.STAYS_IN, "endLabel" -> tokenNames.Room, "count" -> 150)
    )
    seq(res("data"), "indexes") should contain.only(
      Map(
        "labels" -> List(tokenNames.User),
        "properties" -> List(tokenNames.email),
        "totalSize" -> 1000,
        "estimatedUniqueSize" -> 1000,
        "updatesSinceEstimation" -> 0,
        "indexType" -> "RANGE",
        "indexProvider" -> "range-1.0"
      ),
      Map(
        "labels" -> List(tokenNames.User),
        "properties" -> List(tokenNames.lastName),
        "totalSize" -> 500,
        "estimatedUniqueSize" -> 500,
        "updatesSinceEstimation" -> 0,
        "indexType" -> "RANGE",
        "indexProvider" -> "range-1.0"
      ),
      Map(
        "labels" -> List(tokenNames.User),
        "properties" -> List(tokenNames.firstName, tokenNames.lastName),
        "totalSize" -> 300,
        "estimatedUniqueSize" -> 300,
        "updatesSinceEstimation" -> 0,
        "indexType" -> "RANGE",
        "indexProvider" -> "range-1.0"
      ),
      Map(
        "labels" -> List(tokenNames.Room),
        "properties" -> List(tokenNames.hotel, tokenNames.number),
        "totalSize" -> 150,
        "estimatedUniqueSize" -> 50,
        "updatesSinceEstimation" -> 0,
        "indexType" -> "RANGE",
        "indexProvider" -> "range-1.0"
      ),
      Map(
        "labels" -> List(tokenNames.Car),
        "properties" -> List(tokenNames.number),
        "totalSize" -> 120,
        "estimatedUniqueSize" -> 120,
        "updatesSinceEstimation" -> 8,
        "indexType" -> "RANGE",
        "indexProvider" -> "range-1.0"
      )
    )
    seq(res("data"), "constraints") should contain only
      Map("label" -> tokenNames.User, "properties" -> List(tokenNames.email), "type" -> "Uniqueness constraint")
  }

  private def seq(map: AnyRef, key: String): IndexedSeq[AnyRef] =
    map.asInstanceOf[Map[String, AnyRef]](key).asInstanceOf[IndexedSeq[AnyRef]]
}
