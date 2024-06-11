/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package cypher.features

import cypher.features.Neo4jAdapter.defaultTestConfig
import cypher.features.ScenarioTestHelper.checkForDuplicates
import cypher.features.ScenarioTestHelper.isTransientError
import cypher.features.ScenarioTestHelper.parseDenylist
import cypher.features.ScenarioTestHelper.printComputedDenylist
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.neo4j.cypher.internal.util.test_helpers.DenylistEntry
import org.neo4j.cypher.internal.util.test_helpers.FeatureTest
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.opencypher.tools.tck.api.ExpectError
import org.opencypher.tools.tck.api.Scenario

import java.net.URI
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Runs each scenario with neo4j.
 * Implement the abstract methods to configure how to run scenarios.
 */
trait ScenarioTestHelper extends FeatureTest {

  def config: TestConfig

  def dbConfigPerFeature(featureName: String): collection.Map[Setting[_], AnyRef]

  def useBolt: Boolean

  def dbProvider(): TestDatabaseProvider

  private lazy val denylistEntries = config.denylist.map(parseDenylist).getOrElse(Seq.empty[DenylistEntry])

  final override def denylist(): Seq[DenylistEntry] = denylistEntries

  override def runDenyListedScenario(scenario: Scenario): Seq[Executable] = {
    val name = scenario.toString()
    val scenarioExpectsError: Boolean = scenario.steps.exists(_.isInstanceOf[ExpectError])
    val executable: Executable = () => {
      Try {
        scenario(Neo4jAdapter(
          config.executionPrefix,
          dbProvider(),
          dbConfigPerFeature(scenario.featureName),
          useBolt,
          scenario
        )).run()
      } match {
        case Success(_) =>
          if (!config.experimental) {
            if (!denylist().exists(_.isFlaky(scenario)))
              throw new IllegalStateException("Unexpectedly succeeded in the following denylisted scenario:\n" + name)
          }
        case Failure(e) =>
          e.getCause match {
            case cause @ Neo4jExecutionFailed(_, phase, _, _) =>
              // If the scenario expects an error (e.g. at compile time), but we throw it at runtime instead
              // That is not critical. Therefore, if the test is denylisted, we allow it to fail at runtime.
              // If, on the other hand, the scenario expects results and the test is denylisted, only compile
              // time failures are acceptable.
              if (
                phase == Phase.runtime && !scenarioExpectsError &&
                !(denylist().exists(_.acceptTransientError(scenario)) && isTransientError(cause))
              ) {
                // That's not OK
                throw new Exception(
                  s"""Failed at $phase in scenario $name for query
                     |(NOTE: This test is marked as expected to fail, but failing at $phase is not ok)
                     |""".stripMargin,
                  cause.cause
                )
              }
            // else failed as expected
            // Not supported
            case _ =>
            // TODO consider failing here, once we fixed Ordering in pipelined runtime.
            // Wrong results
          }
      }

    }
    Seq(executable)
  }

  final override def runScenario(scenario: Scenario): Seq[Executable] = {
    val runnable = scenario(Neo4jAdapter(
      config.executionPrefix,
      dbProvider(),
      dbConfigPerFeature(scenario.featureName),
      useBolt,
      scenario
    ))
    val executable: Executable = () => runnable.run()
    Seq(executable)
  }

  @Test
  def checkDenyList(): Unit = {
    checkForDuplicates(scenarios, denylist().toList)
    val unusedDenylistEntries = denylist().filterNot(b => scenarios.exists(s => b.isDenylisted(s)))
    if (unusedDenylistEntries.nonEmpty) {
      throw new IllegalStateException("The following entries of the denylist were not found: \n"
        + unusedDenylistEntries.mkString("\n"))
    }
  }

  @Disabled
  def generateDenylist(): Unit = {
    printComputedDenylist(scenarios, config, dbProvider())
    fail("Do not forget to add @Disabled to this method")
  }

  def releaseResources(): Unit = {
    dbProvider().close()
  }
}

object ScenarioTestHelper {

  private def isTransientError(error: Neo4jExecutionFailed): Boolean = {
    error.cause match {
      case c: HasStatus => c.status().code().classification() == Status.Classification.TransientError
      case _            => false
    }
  }

  private def checkForDuplicates(scenarios: Seq[Scenario], denylist: Seq[DenylistEntry]): Unit = {
    // test scenarios
    val testScenarios = new mutable.HashSet[Scenario]()
    for (s <- scenarios) {
      if (testScenarios.contains(s)) {
        throw new IllegalStateException("Multiple scenarios exists for the following name: " + s)
      } else {
        testScenarios += s
      }
    }
    // test denylist
    val testDenylist = new mutable.HashSet[DenylistEntry]()
    for (b <- denylist) {
      if (testDenylist.contains(b)) {
        throw new IllegalStateException("Multiple denylists entries exists for the following scenario: " + b)
      } else {
        testDenylist += b
      }
    }
  }

  private def parseDenylist(denylistPathString: String): Seq[DenylistEntry] = {
    def validate(scenarioName: String): Unit = {
      if (scenarioName.head.isWhitespace || scenarioName.last.isWhitespace) {
        throw new Exception(s"Invalid whitespace in scenario name $scenarioName from file $denylistPathString")
      }
    }

    def loadFile(denylistPathString: String, validate: String => Unit, resourceUri: URI) = {
      val fs =
        if ("jar".equalsIgnoreCase(resourceUri.getScheme)) {
          Some(FileSystems.newFileSystem(resourceUri, new util.HashMap[String, String]))
        } else None

      try {
        val denylistPath: Path = Paths.get(resourceUri)
        val denylistPaths = Files.walk(denylistPath).filter {
          (t: Path) => Files.isRegularFile(t)
        }
        val denylistPathsList: List[Path] = denylistPaths.iterator().asScala.toList
        if (denylistPathsList.isEmpty) throw new NoSuchFileException(s"Denylist file not found at: $denylistPathString")
        val lines = denylistPathsList.flatMap(f => Files.readAllLines(f, StandardCharsets.UTF_8).asScala.toList)

        val scenarios =
          lines.filterNot(line => line.startsWith("//") || line.isEmpty) // comments in denylist are being ignored
        scenarios.foreach(validate)
        val importPattern = "^import (\\S+)$".r
        scenarios.flatMap {
          case importPattern(fileImport) =>
            parseDenylist(Paths.get(denylistPathString).getParent.resolve(fileImport).toString)
          case regularLine =>
            Seq(DenylistEntry(regularLine))
        }
      } finally {
        try {
          fs.foreach(_.close())
        } catch {
          case _: UnsupportedOperationException => ()
        }
      }
    }

    def formatSpecificDenylistURL = {
      val customFormat = System.getProperty("NEO4J_OVERRIDE_STORE_FORMAT")
      val prefix = denylistPathString.lastIndexOf("/")
      val formatSpecificDenyFile = denylistPathString.substring(0, prefix + 1) + customFormat + ".txt"
      getClass.getResource("/" + formatSpecificDenyFile)
    }

    val formatSpecificDenyURL: URL = formatSpecificDenylistURL
    var formatSpecificDenyList = List.empty[DenylistEntry]
    if (formatSpecificDenyURL != null) {
      formatSpecificDenyList = loadFile(denylistPathString, validate, formatSpecificDenyURL.toURI)
    }

    /*
     * Find denylist no matter if thy are in the filesystem or in the a jar.
     * The code get executed from a jar, when the TCK on a public artifact of Neo4j.
     */
    val resourceUrl: URL = getClass.getResource("/" + denylistPathString)
    if (resourceUrl == null) throw new NoSuchFileException(s"Denylist file not found at: $denylistPathString")
    val resourceUri: URI = resourceUrl.toURI

    val denyList = loadFile(denylistPathString, validate, resourceUri)
    (formatSpecificDenyList ++ denyList).distinct
  }

  /*
    This method can be used to generate a denylist for a given TestConfig.
    It can be very useful when adding a new runtime for example.
   */
  private def printComputedDenylist(
    scenarios: Seq[Scenario],
    config: TestConfig,
    dbProvider: TestDatabaseProvider,
    useBolt: Boolean = false
  ): Unit = {
    // Sometime this method doesn't print its progress output (but is actually working (Do not cancel)!).
    // TODO: Investigate this!
    println("Evaluating scenarios")
    val numberOfScenarios = scenarios.size
    val denylist = scenarios.zipWithIndex.flatMap { case (scenario, index) =>
      val isFailure = Try(scenario(Neo4jAdapter(
        config.executionPrefix,
        dbProvider,
        defaultTestConfig(scenario.featureName),
        useBolt,
        scenario
      )).run()).isFailure
      print(s"Processing scenario ${index + 1}/$numberOfScenarios\n")
      Console.out.flush() // to make sure we see progress
      if (isFailure) Some(scenarioToString(scenario)) else None
    }
    // Sort the list alphabetically to normalize diffs
    println(denylist.distinct.sorted.mkString("\n", "\n", "\n"))
  }

  // This is the OpenCypher 1.0.0-M15 + a few months version of Scenario.toString way of formatting, which is used in the denylist
  private def scenarioToString(scenario: Scenario): String =
    s"""Feature "${scenario.featureName}": Scenario "${scenario.name}"""" + scenario.exampleIndex.map(ix =>
      s""": Example "$ix""""
    ).getOrElse("")

}
