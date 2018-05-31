/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.features

import java.nio.file.FileSystems

import org.junit.jupiter.api.AfterEach
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

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
