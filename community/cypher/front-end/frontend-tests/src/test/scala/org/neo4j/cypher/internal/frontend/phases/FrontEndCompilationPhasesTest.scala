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

import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.settingToFeatureMapping
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FrontEndCompilationPhasesTest extends CypherFunSuite {

  // If this test fails, you most likely have forgotten to update defaultSemanticFeatures
  // when turning on a feature flag in GraphDatabaseInternalSettings
  test("default semantic features should match the ones that are true by default in settings") {

    val expectedSemanticFeatures = settingToFeatureMapping.collect {
      case (setting, feature) if setting.defaultValue => feature
    }

    expectedSemanticFeatures.foreach(feature =>
      withClue(
        "A new feature flag has been turned on in GraphDatabaseInternalSettings. " +
          "You need to also update FrontEndCompilationPhases.defaultSemanticFeatures with the corresponding semantic feature. \n"
      ) {
        defaultSemanticFeatures should contain(feature)
      }
    )
  }
}
