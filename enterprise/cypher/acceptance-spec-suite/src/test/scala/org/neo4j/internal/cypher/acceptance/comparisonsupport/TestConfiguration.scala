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
package org.neo4j.internal.cypher.acceptance.comparisonsupport

/**
  * A set of scenarios.
  */
case class TestConfiguration(scenarios: Set[TestScenario]) {

  def +(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios ++ other.scenarios)

  def -(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios -- other.scenarios)

  def /\(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios intersect other.scenarios)

  def containsScenario(scenario: TestScenario): Boolean = this.scenarios.contains(scenario)
}

object TestConfiguration {
  implicit def scenarioToTestConfiguration(scenario: TestScenario): TestConfiguration = TestConfiguration(scenario)

  def apply(scenarios: TestScenario*): TestConfiguration = {
    TestConfiguration(scenarios.toSet)
  }

  def apply(versions: Versions, planners: Planners, runtimes: Runtimes): TestConfiguration = {
    val scenarios = for (v <- versions.versions;
                         p <- planners.planners;
                         r <- runtimes.runtimes)
      yield TestScenario(v, p, r)
    TestConfiguration(scenarios.toSet)
  }

  def apply(description:String): TestConfiguration = {
    // Splitting on "\n" works in both Unix and Windows, since the Strings that come in here are from our source code, which has unix line endings
    val configs = description.split("\n").map(_.trim)
    configs.map { stringDescription =>
      if(stringDescription.isEmpty) {
        empty
      } else {
        val args = stringDescription.split(" ")
        val versions = Versions.definedBy(args)
        val planners = Planners.definedBy(args)
        val runtimes = Runtimes.definedBy(args)
        TestConfiguration(versions, planners, runtimes)
      }
    }.reduceOption(_ + _).getOrElse(Configs.Empty)
  }

  def empty: TestConfiguration = {
    TestConfiguration(Nil: _*)
  }
}
