/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import java.nio.file.FileSystems

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.junit.jupiter.api.AfterEach
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

@RunWith(classOf[JUnitPlatform])
class TCKTests extends BaseFeatureTest {

  // these two should be empty on commit!
  override val featureToRun = ""
  override val scenarioToRun = ""

  override val scenarios: Seq[Scenario] = {
    val allScenarios: Seq[Scenario] = CypherTCK.allTckScenarios.filterNot(testsWithEncodingProblems)
    filterScenarios(allScenarios)
  }

  // those tests run into some utf-8 encoding problems on some of our machines at the moment - fix for that is on the way on TCK side
  // TODO: Remove this filter when new version of TCK (1.0.0-M10) is released
  def testsWithEncodingProblems(scenario: Scenario): Boolean = {
    (scenario.name.equals("Fail for invalid Unicode hyphen in subtraction") && scenario.featureName.equals("SemanticErrorAcceptance"))  ||
      (scenario.name.equals("Accept valid Unicode literal") && scenario.featureName.equals("ReturnAcceptance2"))
  }

  @AfterEach
  def tearDown(): Unit = {
    //TODO: This method can be removed with new release of TCK (1.0.0-M10)
    FileSystems.getFileSystem(CypherTCK.getClass.getResource(CypherTCK.featuresPath).toURI).close()
  }
}
