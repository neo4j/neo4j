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

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.function.Executable
import org.opencypher.tools.tck.api.Scenario

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

object ScenarioTestHelper {
  def createTests(scenarios: Seq[Scenario],
                  config: TestConfig,
                  debugOutput: Boolean = false): util.Collection[DynamicTest] = {
    val blacklist = config.blacklist.map(parseBlacklist).getOrElse(Set.empty[BlacklistEntry])
    checkForDuplicates(scenarios, blacklist.toList)
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
            scenario(Neo4jAdapter(config.executionPrefix)).execute()
          } match {
            case Success(_) => throw new IllegalStateException("Unexpectedly succeeded in the following blacklisted scenario:\n" + name)
            case Failure(e) => // failed as expected
          }
        }
      }
      DynamicTest.dynamicTest("Blacklisted Test: " + name, executable)
    }

    val expectPassTests: Seq[DynamicTest] = expectPass.map { scenario =>
      val name = scenario.toString()
      val executable = scenario(Neo4jAdapter(config.executionPrefix))
      DynamicTest.dynamicTest(name, executable)
    }
    (expectPassTests ++ expectFailTests).asJavaCollection
  }

  def checkForDuplicates(scenarios: Seq[Scenario], blacklist: Seq[BlacklistEntry]): Unit = {
    // test scenarios
    var testScenarios = new mutable.HashSet[Scenario]()
    for (s <- scenarios) {
      if (testScenarios.contains(s)) {
        throw new IllegalStateException("Multiple scenarios exists for the following name: " + s)
      } else {
        testScenarios += s
      }
    }
    // test blacklist
    var testBlacklist = new mutable.HashSet[BlacklistEntry]()
    for (b <- blacklist) {
      if (testBlacklist.contains(b)) {
        throw new IllegalStateException("Multiple blacklists entrys exists for the following scenario: " + b)
      } else {
        testBlacklist += b
      }
    }
  }

  def parseBlacklist(blacklistFile: String): Seq[BlacklistEntry] = {
    def validate(scenarioName: String): Unit = {
      if (scenarioName.head.isWhitespace || scenarioName.last.isWhitespace) {
        throw new Exception(s"Invalid whitespace in scenario name $scenarioName from file $blacklistFile")
      }
    }

    val uri = new URI("/blacklists/" + blacklistFile)
    val url = getClass.getResource(uri.getPath)
    if (url == null) throw new FileNotFoundException(s"Blacklist file not found at: $blacklistFile")
    val lines = Source.fromFile(url.toURI, StandardCharsets.UTF_8.name()).getLines()
    val scenarios = lines.filterNot(line => line.startsWith("//") || line.isEmpty).toList // comments in blacklist are being ignored
    scenarios.foreach(validate)
    scenarios.map(BlacklistEntry(_))
  }

  /*
    This method can be used to generate a blacklist for a given TestConfig.
    It can be very useful when adding a new runtime for example.
   */
  def printComputedBlacklist(scenarios: Seq[Scenario],
                             config: TestConfig): Unit = {
    //Sometime this method doesn't print its progress output (but is actually working (Do not cancel)!).
    //TODO: Investigate this!
    println("Evaluating scenarios")
    val numberOfScenarios = scenarios.size
    val blacklist = scenarios.zipWithIndex.flatMap { case (scenario, index) =>
      val isFailure = Try(scenario(Neo4jAdapter(config.executionPrefix)).execute()).isFailure
      print(s"Processing scenario ${index + 1}/$numberOfScenarios\n")
      Console.out.flush() // to make sure we see progress
      if (isFailure) Some(scenario.toString) else None
    }
    println(blacklist.distinct.mkString("\n","\n","\n"))
  }
}
