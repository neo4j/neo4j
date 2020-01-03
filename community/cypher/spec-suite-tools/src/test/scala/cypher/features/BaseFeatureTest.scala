/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package cypher.features

import java.io.File
import java.net.{URI, URL}

import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

@Execution(ExecutionMode.CONCURRENT)
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

object BaseFeatureTestHolder {
  lazy val allTckScenarios: Seq[Scenario] = CypherTCK.allTckScenarios
  lazy val allAcceptanceScenarios: Seq[Scenario] = {
    val packageURL: URL = classOf[BaseFeatureTest].getProtectionDomain.getCodeSource.getLocation
    val resourcePath: String = "/acceptance/features"

    if (packageURL.toString.contains("jar"))
      CypherTCK.parseClasspathFeatures(resourcePath).flatMap(_.scenarios)
    else {
      val featuresURI: URI = new URL(packageURL.toString + resourcePath).toURI
      CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios)
    }
  }
}
