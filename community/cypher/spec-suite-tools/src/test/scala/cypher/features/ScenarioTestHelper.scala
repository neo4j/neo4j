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
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.util

import cypher.features.Neo4jAdapter.defaultTestConfig
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.function.Executable
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.opencypher.tools.tck.api.ExpectError
import org.opencypher.tools.tck.api.Scenario

import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ScenarioTestHelper {
  def createTests(scenarios: Seq[Scenario],
                  config: TestConfig,
                  graphDatabaseFactory: () => TestDatabaseManagementServiceBuilder,
                  dbConfig: collection.Map[Setting[_], Object],
                  useBolt: Boolean = false,
                  debugOutput: Boolean = false): util.Collection[DynamicTest] = {
    val blacklist = config.blacklist.map(parseBlacklist).getOrElse(Set.empty[BlacklistEntry])
    checkForDuplicates(scenarios, blacklist.toList)
    val (expectFail, expectPass) = scenarios.partition(s => blacklist.exists(_.isBlacklisted(s)))
    if (debugOutput) {
      val unusedBlacklistEntries = blacklist.filterNot(b => expectFail.exists(s => b.isBlacklisted(s)))
      if (unusedBlacklistEntries.nonEmpty) {
        println("The following entries of the blacklist were not found in the blacklist: \n"
          + unusedBlacklistEntries.mkString("\n"))
      }
    }

    val expectFailTests: Seq[DynamicTest] = expectFail.map { scenario =>
      val name = scenario.toString()
      val scenarioExpectsError: Boolean = scenario.steps.exists(_.isInstanceOf[ExpectError])
      val executable = new Executable {
        override def execute(): Unit = {
          Try {
            scenario(Neo4jAdapter(config.executionPrefix, graphDatabaseFactory(), dbConfig, useBolt)).run()
          } match {
            case Success(_) =>
              if (!config.experimental) {
                if (!blacklist.exists(_.isFlaky(scenario)))
                  throw new IllegalStateException("Unexpectedly succeeded in the following blacklisted scenario:\n" + name)
              }
            case Failure(e) =>
              e.getCause match {
                case cause@Neo4jExecutionFailed(_, phase, _, _) =>
                  // If the scenario expects an error (e.g. at compile time), but we throw it at runtime instead
                  // That is not critical. Therefore, if the test is blacklisted, we allow it to fail at runtime.
                  // If, on the other hand, the scenario expects results and the test is blacklisted, only compile
                  // time failures are acceptable.
                  if (phase == Phase.runtime && !scenarioExpectsError) {
                    // That's not OK
                    throw new Exception(
                      s"""Failed at $phase in scenario $name for query
                         |(NOTE: This test is marked as expected to fail, but failing at $phase is not ok)
                         |""".stripMargin, cause.cause)
                  }
                  // else failed as expected
                  // Not supported
                case _ =>
                  // TODO consider failing here, once we fixed Ordering in pipelined runtime.
                  // Wrong results
              }
          }
        }
      }
      DynamicTest.dynamicTest("Blacklisted Test: " + name, executable)
    }

    val expectPassTests: Seq[DynamicTest] = expectPass.map { scenario =>
      val name = scenario.toString()
      val runnable = scenario(Neo4jAdapter(config.executionPrefix, graphDatabaseFactory(), dbConfig, useBolt))
      DynamicTest.dynamicTest(name, () => runnable.run())
    }
    (expectPassTests ++ expectFailTests).asJavaCollection
  }

  def checkForDuplicates(scenarios: Seq[Scenario], blacklist: Seq[BlacklistEntry]): Unit = {
    // test scenarios
    val testScenarios = new mutable.HashSet[Scenario]()
    for (s <- scenarios) {
      if (testScenarios.contains(s)) {
        throw new IllegalStateException("Multiple scenarios exists for the following name: " + s)
      } else {
        testScenarios += s
      }
    }
    // test blacklist
    val testBlacklist = new mutable.HashSet[BlacklistEntry]()
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

    /*
     * Find blacklist no matter if thy are in the filesystem or in the a jar.
     * The code get executed from a jar, when the TCK on a public artifact of Neo4j.
     */
    val resourcePath = "/blacklists/" + blacklistFile
    val resourceUrl: URL = getClass.getResource(resourcePath)
    if (resourceUrl == null) throw new NoSuchFileException(s"Blacklist file not found at: $resourcePath")
    val resourceUri: URI = resourceUrl.toURI
    val fs =
      if ("jar".equalsIgnoreCase(resourceUri.getScheme)) {
        Some(FileSystems.newFileSystem(resourceUri, new util.HashMap[String, String]))
      } else None

    try {
      val blacklistPath: Path = Paths.get(resourceUri)
      val blacklistPaths = Files.walk(blacklistPath).filter {
        t: Path => Files.isRegularFile(t)
      }
      val blacklistPathsList: List[Path] = blacklistPaths.iterator().asScala.toList
      if (blacklistPathsList.isEmpty) throw new NoSuchFileException(s"Blacklist file not found at: $resourcePath")
      val lines = blacklistPathsList.flatMap(f => Files.readAllLines(f, StandardCharsets.UTF_8).asScala.toList)

      val scenarios = lines.filterNot(line => line.startsWith("//") || line.isEmpty) // comments in blacklist are being ignored
      scenarios.foreach(validate)
      scenarios.map(BlacklistEntry(_))
    } finally {
      try {
        fs.foreach(_.close())
      } catch {
        case _: UnsupportedOperationException => Unit
      }
    }
  }

  /*
    This method can be used to generate a blacklist for a given TestConfig.
    It can be very useful when adding a new runtime for example.
   */
  def printComputedBlacklist(scenarios: Seq[Scenario],
                             config: TestConfig, graphDatabaseFactory: () => TestDatabaseManagementServiceBuilder,
                             useBolt: Boolean = false): Unit = {
    //Sometime this method doesn't print its progress output (but is actually working (Do not cancel)!).
    //TODO: Investigate this!
    println("Evaluating scenarios")
    val numberOfScenarios = scenarios.size
    val denylist = scenarios.zipWithIndex.flatMap { case (scenario, index) =>
      val isFailure = Try(scenario(Neo4jAdapter(config.executionPrefix, graphDatabaseFactory(), defaultTestConfig, useBolt)).run()).isFailure
      print(s"Processing scenario ${index + 1}/$numberOfScenarios\n")
      Console.out.flush() // to make sure we see progress
      if (isFailure) Some(scenarioToString(scenario)) else None
    }
    // Sort the list alphabetically to normalize diffs
    println(denylist.distinct.sorted.mkString("\n","\n","\n"))
  }

  // This is the OpenCypher 1.0.0-M15 + a few months version of Scenario.toString way of formatting, which is used in the denylist
  def scenarioToString(scenario: Scenario): String =
    s"""Feature "${scenario.featureName}": Scenario "${scenario.name}"""" + scenario.exampleIndex.map(ix => s""": Example "$ix"""").getOrElse("")

}
