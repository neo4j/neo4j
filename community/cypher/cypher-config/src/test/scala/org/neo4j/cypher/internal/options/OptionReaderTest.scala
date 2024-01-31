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
package org.neo4j.cypher.internal.options

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherParallelRuntimeSupport.DISABLED
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_parallel_runtime_support
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidCypherOption

class OptionReaderTest extends CypherFunSuite {

  test("Can read defaults") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set()
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions)
  }

  test("Can read options from config") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(
        Config.newBuilder()
          .set(GraphDatabaseInternalSettings.cypher_runtime, GraphDatabaseInternalSettings.CypherRuntime.INTERPRETED)
          .build()
      ),
      keyValues = Set()
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        runtime = CypherRuntimeOption.interpreted
      ))
  }

  // If this test fails it means you introduced a debug option that can be set from the Config.
  // Please delete this test and unignore the two following tests.
  // They test that we can read the debug option from a config. You will have to change the tests
  // to set your new config value instead of `cypher_eager_analysis_implementation`.
  test("there are no debug options that can be read from config") {
    CypherDebugOption.cypherConfigBooleans should be(empty)
  }

  ignore("Can read debug options from config") {
    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(
        Config.newBuilder()
          .set(
            GraphDatabaseInternalSettings.cypher_eager_analysis_implementation,
            GraphDatabaseInternalSettings.EagerAnalysisImplementation.LP
          )
          .build()
      ),
      keyValues = Set()
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        debugOptions = CypherDebugOptions(Set( /*CypherDebugOption.useLPEagerAnalyzer*/ ))
      ))
  }

  ignore("Can read debug options from config and key-values") {
    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(
        Config.newBuilder()
          .set(
            GraphDatabaseInternalSettings.cypher_eager_analysis_implementation,
            GraphDatabaseInternalSettings.EagerAnalysisImplementation.LP
          )
          .build()
      ),
      keyValues = Set("debug" -> "toString")
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        debugOptions = CypherDebugOptions(Set( /*CypherDebugOption.useLPEagerAnalyzer,*/ CypherDebugOption.tostring))
      ))
  }

  test("Can read options from key-values") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("planner" -> "dp", "debug" -> "toString", "debug" -> "ast")
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        planner = CypherPlannerOption.dp,
        debugOptions = CypherDebugOptions(Set(CypherDebugOption.tostring, CypherDebugOption.ast))
      ))
  }

  test("Can read options from key-values overriding config") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(
        Config.newBuilder()
          .set(GraphDatabaseInternalSettings.cypher_runtime, GraphDatabaseInternalSettings.CypherRuntime.INTERPRETED)
          .build()
      ),
      keyValues =
        Set("planner" -> "dp", "runtime" -> "slotted", "debug" -> "toString", "debug" -> "ast")
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        planner = CypherPlannerOption.dp,
        runtime = CypherRuntimeOption.slotted,
        debugOptions = CypherDebugOptions(Set(CypherDebugOption.tostring, CypherDebugOption.ast))
      ))
  }

  test("Can read options from key-values with different case") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("PLANNER" -> "dp", "debug" -> "toString", "DEbug" -> "ast")
    )

    options
      .shouldEqual(CypherQueryOptions.defaultOptions.copy(
        planner = CypherPlannerOption.dp,
        debugOptions = CypherDebugOptions(Set(CypherDebugOption.tostring, CypherDebugOption.ast))
      ))
  }

  test("Fails on invalid value") {

    val exception = intercept[InvalidCypherOption](CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("planner" -> "foo")
    ))

    exception.getMessage.should(include("planner") and include("foo"))
  }

  test("Fails on invalid keys") {

    val exception = intercept[InvalidCypherOption](CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("invalid1" -> "bar", "invalid2" -> "bar")
    ))

    exception.getMessage.should(include("invalid1") and include("invalid2"))
  }

  test("Fails on parallel runtime config disabled") {
    val exception = intercept[InvalidCypherOption](CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults(cypher_parallel_runtime_support, DISABLED)),
      keyValues = Set("runtime" -> "parallel")
    ))

    exception.getMessage.should(
      be("Parallel runtime has been disabled, please enable it or upgrade to a bigger Aura instance.")
    )
  }

}
