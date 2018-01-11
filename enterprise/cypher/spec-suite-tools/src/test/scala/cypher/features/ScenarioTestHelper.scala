/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.function.Executable
import org.opencypher.tools.tck.api.Scenario

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

object ScenarioTestHelper {
  def createTests(scenarios: Seq[Scenario],
                  blacklist: Set[BlacklistEntry] = Set.empty,
                  executionPrefix: String = "",
                  debugOutput: Boolean = false): util.Collection[DynamicTest] = {

    val (expectFail, expectPass) = scenarios.partition { s => blacklist.exists(_.isBlacklisted(s)) }
    if (debugOutput) {
      val unusedBlacklistEntries = blacklist.filterNot(b => expectFail.exists(s => b.isBlacklisted(s)))
      if (unusedBlacklistEntries.nonEmpty) {
        println("The following entries of the blacklist were not found in the blacklist: \n"
          + unusedBlacklistEntries.mkString("\n"))
      }
    }

    val expectFailTests: Seq[DynamicTest] = expectFail.map { scenario =>
      val name = scenario.toString()
      val executable = new Executable {
        override def execute(): Unit = {
          Try {
            scenario(Neo4jAdapter(executionPrefix)).execute()
          } match {
            case Success(_) => throw new IllegalStateException("Unexpectedly succeeded in the following blacklisted scenario:\n" + name)
            case Failure(e) => println("Failed as expected with\n  " + e.getMessage)
          }
        }
      }
      DynamicTest.dynamicTest("Blacklisted Test: " + name, executable)
    }

    val expectPassTests: Seq[DynamicTest] = expectPass.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(Neo4jAdapter(executionPrefix))
      DynamicTest.dynamicTest(name, executable)
    }
    (expectPassTests ++ expectFailTests).asJavaCollection
  }

  def parseBlacklist(blacklistFile: String): Set[BlacklistEntry] = {
    def validate(scenarioName: String): Unit = {
      if(scenarioName.head.isWhitespace || scenarioName.last.isWhitespace) {
        throw new Exception(s"Invalid whitespace in scenario name $scenarioName from file $blacklistFile")
      }
    }

    val uri = new URI("/blacklists/" + blacklistFile)

    val url = getClass.getResource(uri.getPath)
    if (url == null) throw new FileNotFoundException(s"blacklist file not found at: $blacklistFile")
    val lines = Source.fromFile(url.getPath, StandardCharsets.UTF_8.name()).getLines()
    val scenarios = lines.filterNot(line => line.startsWith("//") || line.isEmpty).toSet // comments in blacklist are being ignored
    scenarios.foreach(validate)
    scenarios.map(BlacklistEntry(_))
  }

  def printComputedBlacklist(scenarios: Seq[Scenario],
                             executionPrefix: String = ""): List[String] = {
    println("Evaluating scenarios")
    val numberOfScenarios = scenarios.size
    val blacklist = scenarios.zipWithIndex.flatMap { case (scenario, index) =>
      val isFailure = Try(scenario(Neo4jAdapter(executionPrefix)).execute()).isFailure
      println(s"Processing scenario ${index + 1}/$numberOfScenarios")
      if (isFailure) Some(scenario.toString) else None
    }.toList
    println()
    println(blacklist.mkString("\n"))
    blacklist
  }
}
