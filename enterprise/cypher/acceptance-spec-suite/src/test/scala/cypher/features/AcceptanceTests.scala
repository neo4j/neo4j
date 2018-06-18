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

import java.io.File
import java.net.URI

import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

@RunWith(classOf[JUnitPlatform])
class AcceptanceTests extends BaseFeatureTest {

  // these two should be empty on commit!
  // Use a substring match, for example "UnwindAcceptance"
  override val featureToRun = ""
  override val scenarioToRun = ""

  val featuresURI: URI = getClass.getResource("/cypher/features").toURI

  override val scenarios: Seq[Scenario] = {
    val all = CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios)
    filterScenarios(all)
  }
}
