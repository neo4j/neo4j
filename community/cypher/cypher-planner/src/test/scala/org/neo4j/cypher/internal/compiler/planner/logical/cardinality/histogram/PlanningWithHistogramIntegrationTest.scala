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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.histogram

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.NODE_TYPE

class PlanningWithHistogramIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test(
    "Using the histogram should give the planner the knowledge that Person.bYear >= 1963 is much more selective that Person.bYear <= 1963"
  ) {
    val histogram = HistogramTestHelper.createHistogramFromString(
      NODE_TYPE,
      "Person",
      "bYear",
      """
        |+-------------------------------------------------------+
        || minInclusive | maxExclusive | frequency | selectivity |
        |+-------------------------------------------------------+
        || 1960         | 1962         | 2         | 0.02        |
        || 1962         | 1963         | 92        | 0.92        |
        || 1963         | 1987         | 2         | 0.02        |
        || 1987         | 1989         | 2         | 0.02        |
        || 1989         | 1990         | 2         | 0.02        |
        |+-------------------------------------------------------+
        |""".stripMargin
    )

    val plannerBuilderWithoutHistogram = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("Person", 1000)
      .setRelationshipCardinality("()-[:FOLLOWS]->()", 5000)
      .setRelationshipCardinality("(:Person)-[:FOLLOWS]->()", 5000)
      .setRelationshipCardinality("()-[:FOLLOWS]->(:Person)", 5000)
      .setRelationshipCardinality("(:Person)-[:FOLLOWS]->(:Person)", 5000)

    val plannerWithoutHistogram = plannerBuilderWithoutHistogram.build()

    val plannerWithHistogram = plannerBuilderWithoutHistogram.addHistogram(histogram).build()

    val query = {
      """
        |MATCH (many:Person WHERE many.bYear <= 1963)-[manyFollows:FOLLOWS WHERE manyFollows.since < 2025]->
        |  (middle:Person)
        |  <-[:FOLLOWS]-(few:Person WHERE few.bYear >= 1963) RETURN many, middle, few
        |""".stripMargin
    }
    plannerWithoutHistogram.plan(query) should be(
      plannerWithoutHistogram.planBuilder()
        .produceResults("many", "middle", "few")
        .filter("NOT anon_0 = manyFollows", "few.bYear >= 1963", "few:Person")
        .expandAll("(middle)<-[anon_0:FOLLOWS]-(few)")
        .filter("manyFollows.since < 2025", "middle:Person")
        .expandAll("(many)-[manyFollows:FOLLOWS]->(middle)")
        .filter("many.bYear <= 1963")
        .nodeByLabelScan("many", "Person")
        .build()
    )

    plannerWithHistogram.plan(query) should be(
      plannerWithHistogram.planBuilder()
        .produceResults("many", "middle", "few")
        .filter(
          "NOT anon_0 = manyFollows",
          "andsReorderable(many.bYear <= 1963 AND manyFollows.since < 2025)",
          "many:Person"
        )
        .expandAll("(middle)<-[manyFollows:FOLLOWS]-(many)")
        .filter("middle:Person")
        .expandAll("(few)-[anon_0:FOLLOWS]->(middle)")
        .filter("few.bYear >= 1963")
        .nodeByLabelScan("few", "Person")
        .build()
    )
  }

  test(
    "Using the histogram from the config should give the planner the knowledge that Person.bYear >= 1963 is much more selective that Person.bYear <= 1963"
  ) {
    val plannerBuilderWithoutHistogram = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("Person", 1000)
      .setRelationshipCardinality("()-[:FOLLOWS]->()", 5000)
      .setRelationshipCardinality("(:Person)-[:FOLLOWS]->()", 5000)
      .setRelationshipCardinality("()-[:FOLLOWS]->(:Person)", 5000)
      .setRelationshipCardinality("(:Person)-[:FOLLOWS]->(:Person)", 5000)

    val plannerWithoutHistogram = plannerBuilderWithoutHistogram.build()

    val plannerWithHistogram = plannerBuilderWithoutHistogram.withSetting(
      GraphDatabaseInternalSettings.histogram_data,
      GraphDatabaseInternalSettings.HistogramsOfStandardBucketTypeParser.parse(
        """
          |labelOrType=Person;property=bYear;min=1960;max=1962;selectivity=0.02;entityType=node,
          |labelOrType=Person;property=bYear;min=1962;max=1963;selectivity=0.92;entityType=node,
          |labelOrType=Person;property=bYear;min=1963;max=1987;selectivity=0.02;entityType=node,
          |labelOrType=Person;property=bYear;min=1987;max=1989;selectivity=0.02;entityType=node,
          |labelOrType=Person;property=bYear;min=1989;max=1990;selectivity=0.02;entityType=node
          |""".stripMargin
      )
    ).build()

    val query = {
      """
        |MATCH (many:Person WHERE many.bYear <= 1963)-[manyFollows:FOLLOWS WHERE manyFollows.since < 2025]->
        |  (middle:Person)
        |  <-[:FOLLOWS]-(few:Person WHERE few.bYear >= 1963) RETURN many, middle, few
        |""".stripMargin
    }
    plannerWithoutHistogram.plan(query) should be(
      plannerWithoutHistogram.planBuilder()
        .produceResults("many", "middle", "few")
        .filter("NOT anon_0 = manyFollows", "few.bYear >= 1963", "few:Person")
        .expandAll("(middle)<-[anon_0:FOLLOWS]-(few)")
        .filter("manyFollows.since < 2025", "middle:Person")
        .expandAll("(many)-[manyFollows:FOLLOWS]->(middle)")
        .filter("many.bYear <= 1963")
        .nodeByLabelScan("many", "Person")
        .build()
    )

    plannerWithHistogram.plan(query) should be(
      plannerWithHistogram.planBuilder()
        .produceResults("many", "middle", "few")
        .filter(
          "NOT anon_0 = manyFollows",
          andsReorderableAst(propLessThanEqual("many", "bYear", 1963), propLessThan("manyFollows", "since", 2025)),
          "many:Person"
        )
        .expandAll("(middle)<-[manyFollows:FOLLOWS]-(many)")
        .filter("middle:Person")
        .expandAll("(few)-[anon_0:FOLLOWS]->(middle)")
        .filter("few.bYear >= 1963")
        .nodeByLabelScan("few", "Person")
        .build()
    )
  }
}
