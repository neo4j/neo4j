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
package cypher.features

import java.net.URI
import java.net.URL

import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.opencypher.tools.tck.api.CypherTCK
import org.opencypher.tools.tck.api.Scenario

@Execution(ExecutionMode.CONCURRENT)
abstract class BaseFeatureTest {

  def filterScenarios(allScenarios: Seq[Scenario], categoryToRun: String, featureToRun: String, scenarioToRun: String): Seq[Scenario] = {
    allScenarios.filter(s =>
      categoryToRun.isEmpty || s.categories.exists(c => c.contains(categoryToRun))
    ).filter(s =>
      featureToRun.isEmpty || s.featureName.contains(featureToRun)
    ).filter(s =>
      featureToRun.isEmpty || s.name.contains(scenarioToRun)
    )
  }
}

object BaseFeatureTestHolder {

  lazy val allTckScenarios: Seq[Scenario] = CypherTCK.allTckScenarios
  lazy val allAcceptanceScenarios: Seq[Scenario] = {
    val resourcePath: String = "/acceptance/features"

    val featuresURI: URI = getClass.getResource(resourcePath).toURI
    CypherTCK.parseFeatures(featuresURI).flatMap(_.scenarios)
  }
}
