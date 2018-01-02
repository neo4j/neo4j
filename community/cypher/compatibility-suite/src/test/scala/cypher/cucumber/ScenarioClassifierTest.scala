/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package cypher.cucumber

import cypher.FeatureSuiteTest
import cypher.cucumber.classifier.{Scenario, ScenarioClassifier}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuiteLike}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScenarioClassifierTest extends FunSuiteLike with Matchers {
  test("should classify cucumber scenario") {
    import com.novus.salat._
    import com.novus.salat.global._

    val classify = new ScenarioClassifier().classify(classOf[FeatureSuiteTest])
    val json = grater[Scenario].toPrettyJSONArray(classify)

    // println(json)

    json should not be  empty
  }
}
