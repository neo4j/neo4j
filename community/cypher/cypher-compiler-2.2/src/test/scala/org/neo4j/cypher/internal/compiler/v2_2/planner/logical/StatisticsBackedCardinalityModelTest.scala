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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.{CardinalityModelTestHelper, IndependenceCombiner}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

class StatisticsBackedCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport with CardinalityModelTestHelper {

  val allNodes = 733.0
  val personCount = 324
  val relCount = 50
  val rel2Count = 78

  test("query containing a WITH and LIMIT") {
    val i = .1
    givenPattern("MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()").
    withGraphNodes(allNodes).
    withLabel('Person -> i).
    withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
    shouldHavePlannerQueryCardinality(produceCardinalityModel)(
        Math.min(allNodes * (i / allNodes), 10.0) *
        allNodes * (relCount / (i * allNodes))
      )
  }

  test("query containing a WITH and aggregation vol. 2") {
    val patternNodeCrossProduct = allNodes * allNodes
    val labelSelectivity = personCount / allNodes
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = rel2Count / maxRelCount

    val firstQG = patternNodeCrossProduct * labelSelectivity * relSelectivity

    val aggregation = Math.sqrt(firstQG)

    givenPattern("MATCH (a:Person)-[:REL2]->(b) WITH a, count(*) as c MATCH (a)-[:REL]->()").
    withGraphNodes(allNodes).
    withLabel('Person -> personCount).
    withRelationshipCardinality('Person -> 'REL -> 'Person -> relCount).
    withRelationshipCardinality('Person -> 'REL2 -> 'Person -> rel2Count).
    shouldHavePlannerQueryCardinality(produceCardinalityModel)(aggregation * allNodes * relCount / (personCount * allNodes))
  }

  def produceCardinalityModel(in: QueryGraphCardinalityModel): Metrics.CardinalityModel =
    new StatisticsBackedCardinalityModel(in)

  def createCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, semanticTable, combiner)
}
