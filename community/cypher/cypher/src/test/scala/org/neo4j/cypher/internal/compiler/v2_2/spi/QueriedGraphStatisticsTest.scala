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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{PropertyKeyId, RelTypeId, LabelId}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Selectivity, Cardinality}
import org.neo4j.cypher.internal.spi.v2_2.TransactionBoundQueryContext

class QueriedGraphStatisticsTest extends CypherFunSuite with GraphDatabaseTestSupport {

  test("total number of nodes reported correctly for non-empty database") {
    createNode()
    createNode()

    withGraphStatistics { (qtx, stats) =>
      stats.nodesWithLabelCardinality(None) should equal(Cardinality(2))
    }
  }

  test("total number of nodes reported correctly for empty database") {
    withGraphStatistics { (qtx, stats) =>
      stats.nodesWithLabelCardinality(None) should equal(Cardinality(0))
    }
  }

  test("total number of nodes with a label reported correctly for database with some nodes all having that label") {
    createLabeledNode("BAR")
    createLabeledNode("BAR")
    createLabeledNode("BAR")

    withGraphStatistics { (qtx, stats) =>
      val barLabel = LabelId(qtx.getLabelId("BAR"))

      stats.nodesWithLabelCardinality(Some(barLabel)) should equal(Cardinality(3))
    }
  }

  test("total number of nodes with a label reported correctly for model world") {
   createSimpleDogModelWorld()

    withGraphStatistics { (qtx, stats) =>
      val companyLabel = LabelId(qtx.getLabelId("Company"))
      val employeeLabel = LabelId(qtx.getLabelId("Employee"))

      stats.nodesWithLabelCardinality(None) should equal(Cardinality(6))
      stats.nodesWithLabelCardinality(Some(companyLabel)) should equal(Cardinality(1))
      stats.nodesWithLabelCardinality(Some(employeeLabel)) should equal(Cardinality(3))
    }
  }

  test("total number of relationships reported correctly for model world") {
    createSimpleDogModelWorld()

    withGraphStatistics { (qtx, stats) =>
      val companyLabel = LabelId(qtx.getLabelId("Company"))
      val employeeLabel = LabelId(qtx.getLabelId("Employee"))
      val worksAtType = RelTypeId(qtx.getRelTypeId("WORKS_AT"))

      stats.cardinalityByLabelsAndRelationshipType(None, None, None) should equal(Cardinality(7))
      stats.cardinalityByLabelsAndRelationshipType(None, Some(worksAtType), None) should equal(Cardinality(4))
      stats.cardinalityByLabelsAndRelationshipType(Some(employeeLabel), None, None) should equal(Cardinality(5))
      stats.cardinalityByLabelsAndRelationshipType(None, None, Some(companyLabel)) should equal(Cardinality(4))
      stats.cardinalityByLabelsAndRelationshipType(None, Some(worksAtType), Some(companyLabel)) should equal(Cardinality(4))
      stats.cardinalityByLabelsAndRelationshipType(Some(employeeLabel), Some(worksAtType), None) should equal(Cardinality(3))
      stats.cardinalityByLabelsAndRelationshipType(Some(employeeLabel), Some(worksAtType), Some(companyLabel)) should equal(Cardinality(3))
    }
  }

  test("reports the number of nodes with a given label that have a certain property when there is no index") {
    createRegularIndexModelWorld()

    withGraphStatistics { (qtx, stats) =>
      val xLabel = LabelId(qtx.getLabelId("X"))
      val pkId = PropertyKeyId(qtx.getPropertyKeyId("age"))

      stats.indexSelectivity(xLabel, pkId) should equal(None)
    }
  }

  test("reports the number of nodes with a given label that have a certain property when there is a regular index") {
    createRegularIndexModelWorld()
    graph.createIndex("X", "age")

    withGraphStatistics { (qtx, stats) =>
      val xLabel = LabelId(qtx.getLabelId("X"))
      val pkId = PropertyKeyId(qtx.getPropertyKeyId("age"))

      stats.indexSelectivity(xLabel, pkId) should equal(Some(Selectivity(1.0/3.0)))
    }
  }

  test("reports the number of nodes with a given label that have a certain property when there is an unique index") {
    createUniqueIndexModelWorld()
    graph.createConstraint("X", "age")

    withGraphStatistics { (qtx, stats) =>
      val xLabel = LabelId(qtx.getLabelId("X"))
      val pkId = PropertyKeyId(qtx.getPropertyKeyId("age"))

      stats.indexSelectivity(xLabel, pkId) should equal(Some(Selectivity(1.0/4.0)))
    }
  }

  def withGraphStatistics[T](f: (TokenContext, GraphStatistics) => T): T = {
    val tx = graph.beginTx()
    try {
      val queryContext = new TransactionBoundQueryContext(graph, tx, true, statement)
      val stats = new QueriedGraphStatistics(graph, queryContext)
      val result = f(queryContext, stats)
      tx.success()
      result
    } finally {
      tx.close()
    }
  }

  def createSimpleDogModelWorld() {
    val neo = createLabeledNode(Map("name"->"Neo Technology"), "Company")
    val john = createLabeledNode(Map("name"->"John"), "Employee")
    val sara = createLabeledNode(Map("name"->"Sara", "age"->13), "Employee")
    val guru = createLabeledNode(Map("name"->"Guru", "age"->25), "Employee")
    val dog1 = createNode("name"->"voof")
    val dog2 = createNode("name"->"voof2")

    relate(john, neo, "WORKS_AT")
    relate(sara, neo, "WORKS_AT")
    relate(guru, neo, "WORKS_AT")
    relate(dog1, neo, "WORKS_AT")
    relate(sara, dog1, "OWNS_DOG")
    relate(guru, dog2, "OWNS_DOG")
    relate(dog1, dog2, "LOVES")
  }

  def createRegularIndexModelWorld() = {
    createLabeledNode(Map("name" -> "1", "age" -> 20), "X")
    createLabeledNode(Map("name" -> "2", "age" -> 20), "X")
    createLabeledNode(Map("name" -> "3", "age" -> 23), "X")
    createLabeledNode(Map("name" -> "4", "age" -> 42), "X")
    createLabeledNode(Map("name" -> "5"), "X")
  }


  def createUniqueIndexModelWorld() = {
    createLabeledNode(Map("name" -> "1", "age" -> 20), "X")
    createLabeledNode(Map("name" -> "3", "age" -> 23), "X")
    createLabeledNode(Map("name" -> "4", "age" -> 42), "X")
    createLabeledNode(Map("name" -> "5", "age" -> 86), "X")
    createLabeledNode(Map("name" -> "6"), "X")
  }
}
