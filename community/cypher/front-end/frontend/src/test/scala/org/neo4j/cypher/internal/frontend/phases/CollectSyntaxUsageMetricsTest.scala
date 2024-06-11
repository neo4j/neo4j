/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CollectSyntaxUsageMetricsTest extends CypherFunSuite {
  private val pipeline = Cypher5Parsing andThen CollectSyntaxUsageMetrics

  test("should find multiple things in one query") {
    val stats = runPipeline(
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |MATCH ANY SHORTEST (c)-->*(d)
        |WITH shortestPath( (a)-[*]->(d) ) AS p
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(2)
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  test("should find GPM SHORTEST") {
    val stats = runPipeline(
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.GPM_SHORTEST) should be(1)
  }

  test("should find LEGACY SHORTEST in MATCH") {
    val stats = runPipeline(
      """
        |MATCH shortestPath( (a)-[*]->(b) )
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  test("should find LEGACY SHORTEST in WITH") {
    val stats = runPipeline(
      """
        |MATCH (a), (b) WITH shortestPath( (a)-[*]->(b) ) AS p
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.LEGACY_SHORTEST) should be(1)
  }

  test("should find COLLECT subquery") {
    val stats = runPipeline(
      """
        |RETURN COLLECT { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.COLLECT_SUBQUERY) should be(1)
  }

  test("should find COUNT subquery") {
    val stats = runPipeline(
      """
        |RETURN COUNT { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.COUNT_SUBQUERY) should be(1)
  }

  test("should find EXISTS subquery") {
    val stats = runPipeline(
      """
        |RETURN EXISTS { MATCH (a) RETURN a } AS as
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.EXISTS_SUBQUERY) should be(1)
  }

  test("should find QPP") {
    val stats = runPipeline(
      """
        |MATCH ANY SHORTEST (a)( ()-->() )*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
  }

  test("should find QPP syntactic sugar") {
    val stats = runPipeline(
      """
        |MATCH ANY SHORTEST (a)-->*(b)
        |RETURN *
        |""".stripMargin
    )
    stats.getSyntaxUsageCount(SyntaxUsageMetricKey.QUANTIFIED_PATH_PATTERN) should be(1)
  }

  private def runPipeline(query: String): InternalSyntaxUsageStats = {
    val startState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
    val context = new ErrorCollectingContext() {
      override val internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStats.newImpl()
    }
    pipeline.transform(startState, context)

    context.internalSyntaxUsageStats
  }

}
