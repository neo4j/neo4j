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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.cypher.internal.planner.spi.histogram.StandardBucket
import org.neo4j.exceptions.InvalidArgumentException

class HistogramFromConfigHelperTest extends CypherPlannerTestSuite {

  test("Special characters in the label-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "labelOrType",
          "<html>Hello</html>"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '<html>Hello</html>' for labelOrType. Expected 'ALPHANUMERIC STRING'."
  }

  test("Special characters in the property-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "property",
          "//prop"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '//prop' for property. Expected 'ALPHANUMERIC STRING'."
  }

  test("A non-double value in the min-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "min",
          "value"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input 'value' for min. Expected 'DOUBLE'."
  }

  test("A non-double value in the max-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "max",
          "value"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input 'value' for max. Expected 'DOUBLE'."
  }

  test("A non-double value in the selectivity-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "selectivity",
          "value"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input 'value' for selectivity. Expected 'DOUBLE between 0.0 and 1.0'."
  }

  test("A value larger than 1.0 in the selectivity-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "selectivity",
          "1.1"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '1.1' for selectivity. Expected 'DOUBLE between 0.0 and 1.0'."
  }

  test("A value lower than 0.0 in the selectivity-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "selectivity",
          "-0.1"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '-0.1' for selectivity. Expected 'DOUBLE between 0.0 and 1.0'."
  }

  test("A wrong value in the entityType-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.of(
          "entityType",
          "value"
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input 'value' for entityType. Expected 'node' or 'relationship'."
  }

  test("A missing value for the labelOrType-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.ofEntries(
          java.util.Map.entry("entityType", "node"),
          java.util.Map.entry("property", "prop"),
          java.util.Map.entry("min", "1"),
          java.util.Map.entry("max", "2"),
          java.util.Map.entry("selectivity", "0.3")
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '' for labelOrType. Expected 'non-empty STRING'."
  }

  test("A missing value for the property-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.ofEntries(
          java.util.Map.entry("entityType", "node"),
          java.util.Map.entry("labelOrType", "lbl"),
          java.util.Map.entry("min", "1"),
          java.util.Map.entry("max", "2"),
          java.util.Map.entry("selectivity", "0.3")
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '' for property. Expected 'non-empty STRING'."
  }

  test("A missing value for the selectivity-field from the config should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(java.util.Set.of(java.util.Map.ofEntries(
          java.util.Map.entry("entityType", "node"),
          java.util.Map.entry("labelOrType", "lbl"),
          java.util.Map.entry("property", "prop"),
          java.util.Map.entry("min", "1"),
          java.util.Map.entry("max", "2")
        )))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '' for selectivity. Expected 'FLOAT between 0.0 and 1.0'."
  }

  test("A max value that is not larger than the min value should throw an error") {
    val exception =
      intercept[InvalidArgumentException](
        HistogramsFromConfigHelper.getHistogramsFromConfig(parseConfig(
          """
            |entityType=node;labelOrType=Person;property=bYear;min=1955;max=1950;selectivity=0.05
            |""".stripMargin
        ))
      )
    exception.gqlStatus() shouldBe "22N04"
    exception.gqlStatusObject().statusDescription() shouldBe "error: data exception - invalid input value. Invalid input '1950.0' for max. Expected 'value larger than 1955.0'."
  }

  test("Should create a single histogram from the configuration - nodeType,Person,bYear") {
    val histograms = HistogramsFromConfigHelper.getHistogramsFromConfig(parseConfig(
      """
        |entityType=node;labelOrType=Person;property=bYear;min=1950;max=1960;selectivity=0.05,
        |entityType=node;labelOrType=Person;property=bYear;min=1960;max=1990;selectivity=0.55,
        |entityType=node;labelOrType=Person;property=bYear;min=1990;max=2010;selectivity=0.40,
        |""".stripMargin
    ))
    histograms.size shouldBe 1
    val histogram = histograms.head
    histogram.nodeOrRelationship shouldBe NODE_TYPE
    histogram.labelOrTypeName shouldBe "Person"
    histogram.property shouldBe "bYear"
    histogram.buckets.size shouldBe 3
    histogram.buckets.contains(StandardBucket(1950, 1960, 0.05)) shouldBe true
    histogram.buckets.contains(StandardBucket(1960, 1990, 0.55)) shouldBe true
    histogram.buckets.contains(StandardBucket(1990, 2010, 0.40)) shouldBe true
  }

  test("Should create a single histogram from the configuration - relationshipType,FOLLOWS,since") {
    val histograms = HistogramsFromConfigHelper.getHistogramsFromConfig(parseConfig(
      """
        |entityType=relationship;labelOrType=FOLLOWS;property=since;min=1950;max=1960;selectivity=0.05,
        |entityType=relationship;labelOrType=FOLLOWS;property=since;min=1960;max=1990;selectivity=0.55,
        |entityType=relationship;labelOrType=FOLLOWS;property=since;min=1990;max=2010;selectivity=0.40,
        |""".stripMargin
    ))
    histograms.size shouldBe 1
    val histogram = histograms.head
    histogram.nodeOrRelationship shouldBe RELATIONSHIP_TYPE
    histogram.labelOrTypeName shouldBe "FOLLOWS"
    histogram.property shouldBe "since"
    histogram.buckets.size shouldBe 3
    histogram.buckets.contains(StandardBucket(1950, 1960, 0.05)) shouldBe true
    histogram.buckets.contains(StandardBucket(1960, 1990, 0.55)) shouldBe true
    histogram.buckets.contains(StandardBucket(1990, 2010, 0.40)) shouldBe true
  }

  test(
    "Should create five histograms from the configuration based on the following groupings: - nodeType,Person,bYear - relationshipType,KNOWS,since - relationshipType,FOLLOWS,since - relationshipType,FOLLOWS,prop"
  ) {
    val histograms = HistogramsFromConfigHelper.getHistogramsFromConfig(parseConfig(
      """
        |entityType=node;labelOrType=Person;property=bYear;min=1950;max=1960;selectivity=0.05,
        |entityType=node;labelOrType=Person;property=bYear;min=1960;max=1990;selectivity=0.0,
        |entityType=node;labelOrType=Person;property=bYear;min=1990;max=2010;selectivity=0.95,
        |entityType=relationship;labelOrType=KNOWS;property=since;min=1900;max=1999;selectivity=0.10,
        |entityType=relationship;labelOrType=KNOWS;property=since;min=1999;max=2012;selectivity=0.90,
        |entityType=relationship;labelOrType=FOLLOWS;property=since;min=1990;max=2010;selectivity=0.40,
        |entityType=relationship;labelOrType=FOLLOWS;property=since;min=2010;max=2020;selectivity=0.60,
        |entityType=relationship;labelOrType=FOLLOWS;property=prop;min=1995;max=2015;selectivity=0.45,
        |entityType=relationship;labelOrType=FOLLOWS;property=prop;min=2015;max=2024;selectivity=0.55,
        |entityType=relationship;labelOrType=SOMETHING;property=prop;min=2015;max=2024;selectivity=1.0
        |""".stripMargin
    ))
    histograms.size shouldBe 5
    val personBYearHistogram = Histogram(
      NODE_TYPE,
      "Person",
      "bYear",
      scala.collection.immutable.Set(
        StandardBucket(1950, 1960, 0.05),
        StandardBucket(1960, 1990, 0.0),
        StandardBucket(1990, 2010, 0.95)
      )
    )
    val knowsSinceHistogram = Histogram(
      RELATIONSHIP_TYPE,
      "KNOWS",
      "since",
      scala.collection.immutable.Set(StandardBucket(1900, 1999, 0.10), StandardBucket(1999, 2012, 0.90))
    )
    val followsSinceHistogram = Histogram(
      RELATIONSHIP_TYPE,
      "FOLLOWS",
      "since",
      scala.collection.immutable.Set(StandardBucket(1990, 2010, 0.40), StandardBucket(2010, 2020, 0.60))
    )
    val followsPropHistogram = Histogram(
      RELATIONSHIP_TYPE,
      "FOLLOWS",
      "prop",
      scala.collection.immutable.Set(StandardBucket(1995, 2015, 0.45), StandardBucket(2015, 2024, 0.55))
    )
    val somethingPropHistogram = Histogram(
      RELATIONSHIP_TYPE,
      "SOMETHING",
      "prop",
      scala.collection.immutable.Set(StandardBucket(2015, 2024, 1.0))
    )
    histograms.contains(personBYearHistogram) shouldBe true
    histograms.contains(knowsSinceHistogram) shouldBe true
    histograms.contains(followsSinceHistogram) shouldBe true
    histograms.contains(followsPropHistogram) shouldBe true
    histograms.contains(somethingPropHistogram) shouldBe true
  }

  def parseConfig(configStringValue: String): java.util.Set[java.util.Map[String, String]] = {
    GraphDatabaseInternalSettings.HistogramsOfStandardBucketTypeParser.parse(configStringValue)
  }
}
