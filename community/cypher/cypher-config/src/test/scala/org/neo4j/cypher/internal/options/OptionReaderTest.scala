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
package org.neo4j.cypher.internal.options

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OptionReaderTest extends CypherFunSuite {

  test("Can read defaults") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set()
    )

    options
      .shouldEqual(CypherQueryOptions.default)
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
      .shouldEqual(CypherQueryOptions.default.copy(
        runtime = CypherRuntimeOption.interpreted
      ))
  }

  // Unignore and change these two tests if adding something to
  //  CypherDebugOption.cypherConfigBooleans. The Map is empty at the time of writing, so we cannot test for this.
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
      .shouldEqual(CypherQueryOptions.default.copy(
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
      .shouldEqual(CypherQueryOptions.default.copy(
        debugOptions = CypherDebugOptions(Set( /*CypherDebugOption.useLPEagerAnalyzer,*/ CypherDebugOption.tostring))
      ))
  }

  test("Can read options from key-values") {

    val options = CypherQueryOptions.fromValues(
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("planner" -> "dp", "debug" -> "toString", "debug" -> "ast")
    )

    options
      .shouldEqual(CypherQueryOptions.default.copy(
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
      .shouldEqual(CypherQueryOptions.default.copy(
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
      .shouldEqual(CypherQueryOptions.default.copy(
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
      config = CypherConfiguration.fromConfig(Config.defaults()),
      keyValues = Set("runtime" -> "parallel")
    ))

    exception.getMessage.should(
      be("Cannot use RUNTIME 'parallel' with 'internal.cypher.parallel_runtime_support:disabled'.")
    )
  }

}
