/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package cypher.features

import org.opencypher.tools.tck.api.Scenario

abstract class BaseFeatureTest {

  def filterScenarios(allScenarios: Seq[Scenario], featureToRun: String, scenarioToRun: String): Seq[Scenario] = {
    if (featureToRun.nonEmpty) {
      val filteredFeature = allScenarios.filter(s => s.featureName.contains(featureToRun))
      if (scenarioToRun.nonEmpty) {
        filteredFeature.filter(s => s.name.contains(scenarioToRun))
      } else
        filteredFeature
    } else if (scenarioToRun.nonEmpty) {
      allScenarios.filter(s => s.name.contains(scenarioToRun))
    } else
      allScenarios
  }

}
